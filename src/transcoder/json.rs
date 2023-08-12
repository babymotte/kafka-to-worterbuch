use async_trait::async_trait;
use miette::miette;

use super::Transcoder;

#[derive(Default)]
pub struct JsonTranscoder;

#[async_trait]
impl Transcoder for JsonTranscoder {
    async fn transcode(
        &self,
        _message_payload: Option<Vec<u8>>,
    ) -> miette::Result<serde_json::Value> {
        Err(miette!("json transocder not yet implemented"))
    }
}
