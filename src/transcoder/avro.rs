use miette::miette;
use rdkafka::message::BorrowedMessage;

use super::Transcoder;

#[derive(Default)]
pub struct AvroTranscoder;

impl Transcoder for AvroTranscoder {
    fn transcode(&self, _message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        Err(miette!("avro transocder not yet implemented"))
    }
}
