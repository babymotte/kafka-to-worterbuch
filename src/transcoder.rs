use self::{
    avro::AvroTranscoder, json::JsonTranscoder, plaintext::PlainTextTranscoder,
    protobuf::ProtobufTranscoder,
};
use crate::instance_manager::{ApplicationManifest, Encoding};
use async_trait::async_trait;
use miette::Result;
use rdkafka::message::BorrowedMessage;
use serde_json::Value;

mod avro;
mod json;
mod plaintext;
mod protobuf;

#[async_trait]
pub trait Transcoder {
    async fn transcode(&self, message: &BorrowedMessage<'_>) -> Result<Value>;
}

pub enum TranscoderImpl<'a> {
    Avro(AvroTranscoder<'a>),
    Json(JsonTranscoder),
    Protobuf(ProtobufTranscoder),
    PlainText(PlainTextTranscoder),
}

#[async_trait]
impl<'a> Transcoder for TranscoderImpl<'a> {
    async fn transcode(&self, message: &BorrowedMessage<'_>) -> Result<Value> {
        match self {
            TranscoderImpl::Avro(transcoder) => transcoder.transcode(message).await,
            TranscoderImpl::Json(transcoder) => transcoder.transcode(message).await,
            TranscoderImpl::Protobuf(transcoder) => transcoder.transcode(message).await,
            TranscoderImpl::PlainText(transcoder) => transcoder.transcode(message).await,
        }
    }
}

pub fn transcoder_for(manifest: &ApplicationManifest) -> impl Transcoder {
    match manifest.encoding {
        Encoding::Avro => TranscoderImpl::Avro(AvroTranscoder::with_schema_registry(
            &manifest.schema_registry,
        )),
        Encoding::Json => TranscoderImpl::Json(JsonTranscoder::default()),
        Encoding::Protobuf => TranscoderImpl::Protobuf(ProtobufTranscoder::default()),
        Encoding::PlainText => TranscoderImpl::PlainText(PlainTextTranscoder::default()),
    }
}
