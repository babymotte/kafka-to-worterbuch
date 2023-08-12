use super::Transcoder;
use async_trait::async_trait;
use serde_json::json;

#[derive(Default)]
pub struct PlainTextTranscoder;

#[async_trait]
impl Transcoder for PlainTextTranscoder {
    async fn transcode(
        &self,
        message_payload: Option<Vec<u8>>,
    ) -> miette::Result<serde_json::Value> {
        if let Some(payload) = message_payload {
            let value = String::from_utf8_lossy(&payload);
            Ok(json!(value))
        } else {
            Ok(json!(""))
        }
    }
}
