use rdkafka::{message::BorrowedMessage, Message};
use serde_json::json;

use super::Transcoder;

#[derive(Default)]
pub struct PlainTextTranscoder;

impl Transcoder for PlainTextTranscoder {
    fn transcode(&self, message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        if let Some(payload) = message.payload() {
            let value = String::from_utf8_lossy(payload);
            Ok(json!(value))
        } else {
            Ok(json!(""))
        }
    }
}
