use self::{
    avro::AvroTranscoder, json::JsonTranscoder, plaintext::PlainTextTranscoder,
    protobuf::ProtobufTranscoder,
};
use crate::instance_manager::Encoding;
use miette::Result;
use rdkafka::message::BorrowedMessage;
use serde_json::Value;

mod avro;
mod json;
mod plaintext;
mod protobuf;

pub trait Transcoder {
    fn transcode(&self, message: &BorrowedMessage<'_>) -> Result<Value>;
}

pub enum TranscoderImpl {
    Avro(AvroTranscoder),
    Json(JsonTranscoder),
    Protobuf(ProtobufTranscoder),
    PlainText(PlainTextTranscoder),
}

impl Transcoder for TranscoderImpl {
    fn transcode(&self, message: &BorrowedMessage<'_>) -> Result<Value> {
        match self {
            TranscoderImpl::Avro(transcoder) => transcoder.transcode(message),
            TranscoderImpl::Json(transcoder) => transcoder.transcode(message),
            TranscoderImpl::Protobuf(transcoder) => transcoder.transcode(message),
            TranscoderImpl::PlainText(transcoder) => transcoder.transcode(message),
        }
    }
}

pub fn transcoder_for(encoding: &Encoding) -> impl Transcoder {
    match encoding {
        Encoding::Avro => TranscoderImpl::Avro(AvroTranscoder::default()),
        Encoding::Json => TranscoderImpl::Json(JsonTranscoder::default()),
        Encoding::Protobuf => TranscoderImpl::Protobuf(ProtobufTranscoder::default()),
        Encoding::PlainText => TranscoderImpl::PlainText(PlainTextTranscoder::default()),
    }
}
