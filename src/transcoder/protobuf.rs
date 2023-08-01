use async_trait::async_trait;
use miette::miette;
use rdkafka::message::BorrowedMessage;

use super::Transcoder;

#[derive(Default)]
pub struct ProtobufTranscoder;

#[async_trait]
impl Transcoder for ProtobufTranscoder {
    async fn transcode(&self, _message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        Err(miette!("protobuf transocder not yet implemented"))
    }
}
