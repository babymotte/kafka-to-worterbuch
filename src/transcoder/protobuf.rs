use super::Transcoder;
use async_trait::async_trait;
use miette::miette;

#[derive(Default)]
pub struct ProtobufTranscoder;

#[async_trait]
impl Transcoder for ProtobufTranscoder {
    async fn transcode(
        &self,
        _message_payload: Option<Vec<u8>>,
    ) -> miette::Result<serde_json::Value> {
        Err(miette!("protobuf transocder not yet implemented"))
    }
}
