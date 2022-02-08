use etl_core::deps::{anyhow, async_trait, log};
use etl_core::queue::QueueClient;
use rusoto_core::{HttpClient, Region};
use rusoto_credential::ChainProvider;
use rusoto_sqs::{SendMessageRequest, Sqs, SqsClient};
use serde::Serialize;
use std::fmt::Debug;

pub struct AwsSqsClient {
    pub client: SqsClient,
    pub queue_url: String,
}

impl AwsSqsClient {
    /// Creates a default client in UsEast1 region using the ChainProvider
    /// Note that message_deduplication_id and message_group_id is set to 
    /// hex(md5(payload-json-string))
    pub fn create<S: ToString>(url: S) -> anyhow::Result<Self> {
        Ok(AwsSqsClient {
            client: SqsClient::new_with(HttpClient::new()?, ChainProvider::new(), Region::UsEast1),
            queue_url: url.to_string(),
        })
    }
}

#[async_trait]
impl<T: Sync + Send + Debug + Serialize + 'static> QueueClient<T> for AwsSqsClient {
    async fn pop(&self) -> anyhow::Result<Option<T>> {
        panic!("QueueClient::pop for AwsSqsClient is not implemented");
    }
    async fn push(&self, m: T) -> anyhow::Result<()> {
        use md5::{Digest, Md5};
        let message_body = serde_json::to_string(&m)?;
        let payload_md5_hash = hex::encode(Md5::new().chain(&message_body).finalize().as_slice());
        match self
            .client
            .send_message(SendMessageRequest {
                message_attributes: None,
                message_deduplication_id: Some(payload_md5_hash.clone()),
                message_group_id: Some(payload_md5_hash),
                queue_url: self.queue_url.clone(),
                message_body,
                ..Default::default()
            })
            .await
        {
            Ok(r) => {
                log::info!("sent message ok: {:?}", r);
            }
            Err(e) => {
                log::error!("{}", e);
                return Err(anyhow::anyhow!("Could not send to queue: {}", e));
            }
        }

        Ok(())
    }
}
