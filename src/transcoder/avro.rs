use super::Transcoder;
use async_trait::async_trait;
use miette::{miette, IntoDiagnostic, Result};
use schema_registry_converter::async_impl::{avro::AvroDecoder, schema_registry::SrSettings};
use url::Url;

pub struct AvroTranscoder<'a> {
    decoder: AvroDecoder<'a>,
}

impl<'a> AvroTranscoder<'a> {
    pub fn with_schema_registry(url: &Option<Url>) -> Result<AvroTranscoder<'a>> {
        let mut url = url
            .as_ref()
            .ok_or_else(|| miette!("cannot create avro transcoder without schema registry"))?
            .to_string();
        if url.ends_with('/') {
            url.pop();
        }
        log::info!("Creating Avro decoder for schema registry {url}");
        let decoder = AvroDecoder::new(SrSettings::new(url));
        Ok(AvroTranscoder { decoder })
    }
}

#[async_trait]
impl<'a> Transcoder for AvroTranscoder<'a> {
    async fn transcode(
        &self,
        message_payload: Option<Vec<u8>>,
    ) -> miette::Result<serde_json::Value> {
        let decoded = self
            .decoder
            .decode(message_payload.as_deref())
            .await
            .into_diagnostic()?;
        decoded.value.try_into().into_diagnostic()
    }
}
