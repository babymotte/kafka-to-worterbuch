use miette::miette;
use rdkafka::message::BorrowedMessage;

use super::Transcoder;

#[derive(Default)]
pub struct JsonTranscoder;

impl Transcoder for JsonTranscoder {
    fn transcode(&self, _message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        Err(miette!("json transocder not yet implemented"))
    }
}
