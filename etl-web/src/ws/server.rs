// using https://github.com/actix/examples/blob/master/websockets/chat-tcp/src/session.rs example
// as template
// also a nice article: https://blog.devgenius.io/lets-build-a-websockets-project-with-rust-and-yew-0-19-60720367399f
use super::event::*;
use rand::{self, rngs::ThreadRng, Rng};
use std::collections::{HashMap, HashSet};

pub struct NotifyClient {
    client_id: usize,
    msg: Notification,
}

pub struct EventServer {
    sessions: HashMap<usize, Recipient<Message>>,
    rng: ThreadRng,
}
