use miette::miette;
use rdkafka::message::BorrowedMessage;

use super::Transcoder;

#[derive(Default)]
pub struct ProtobufTranscoder;

impl Transcoder for ProtobufTranscoder {
    fn transcode(&self, _message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        Err(miette!("protobuf transocder not yet implemented"))
    }
}
