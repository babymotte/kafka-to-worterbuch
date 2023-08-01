use super::Transcoder;
use async_trait::async_trait;
use miette::IntoDiagnostic;
use rdkafka::{message::BorrowedMessage, Message};
use schema_registry_converter::async_impl::{avro::AvroDecoder, schema_registry::SrSettings};
use url::Url;

pub struct AvroTranscoder<'a> {
    decoder: AvroDecoder<'a>,
}

impl<'a> AvroTranscoder<'a> {
    pub fn with_schema_registry(url: &Url) -> AvroTranscoder<'a> {
        let mut url = url.to_string();
        if url.ends_with('/') {
            url.pop();
        }
        log::info!("Creating Avro decoder for schema registry {url}");
        let decoder = AvroDecoder::new(SrSettings::new(String::from(url)));
        AvroTranscoder { decoder }
    }
}

#[async_trait]
impl<'a> Transcoder for AvroTranscoder<'a> {
    async fn transcode(&self, message: &BorrowedMessage<'_>) -> miette::Result<serde_json::Value> {
        let decoded = self
            .decoder
            .decode(message.payload())
            .await
            .into_diagnostic()?;
        decoded.value.try_into().into_diagnostic()
    }
}
