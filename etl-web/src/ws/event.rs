
#[derive(Message)]
#[rtype(result = "()")]
enum SimpleStoreEvent {
    WriteBytes { key: String, payload: Bytes },
    LoadBytes { key: String },
    SimpleStoreError { key: String, error: String },
}

#[derive(Message)]
#[rtype(result = "()")]
/// Sent to client
pub enum Notification {
    SimpleStore(SimpleStoreEvent),
    Pong,
}

pub enum SimpleStoreRequest {
    ListKeys,
}

#[derive(Message)]
#[rtype(result = "()")]
pub enum ClientRequest {
    /// will expand on an enum, like list keys, load bytes
    SimpleStore(SimpleStoreRequest),
    Ping,
}
