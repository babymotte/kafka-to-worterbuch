use async_trait::async_trait;
use miette::miette;
use rdkafka::message::BorrowedMessage;

use super::Transcoder;

#[derive(Default)]
pub struct JsonTranscoder;

#[async_trait]
impl Transcoder for JsonTranscoder {
    async fn transcode(&self, _message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        Err(miette!("json transocder not yet implemented"))
    }
}
