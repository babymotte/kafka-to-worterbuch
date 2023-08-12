use crate::{transcoder::Transcoder, ROOT_KEY};
use miette::{IntoDiagnostic, Result};
use serde_json::{json, Value};
use std::{
    collections::{hash_map::Entry, HashMap},
    time::Instant,
};
use tokio::sync::mpsc;
use worterbuch_client::{topic, Key};

pub struct StateAccumulator<'a, T: Transcoder> {
    transcoder: &'a T,
    topics: HashMap<String, State>,
    application: String,
    wb_message_tx: mpsc::UnboundedSender<(Option<(String, i32, i64)>, (Key, Value))>,
    last_log: Instant,
    topics_synced_key: String,
}

#[derive(Debug, Clone)]
struct State {
    high_watermark: i64,
    key_values: HashMap<String, (i32, Option<Vec<u8>>)>,
    next_offset: i64,
}

impl<'a, T: Transcoder> StateAccumulator<'a, T> {
    pub fn new(
        application: String,
        transcoder: &'a T,
        watermarks: Vec<((String, i32), (i64, i64))>,
        wb_message_tx: mpsc::UnboundedSender<(Option<(String, i32, i64)>, (Key, Value))>,
    ) -> Result<StateAccumulator<'a, T>> {
        let mut topics: HashMap<String, State> = HashMap::new();

        for ((topic, _), (stored_offset, high)) in watermarks {
            match topics.entry(topic) {
                Entry::Occupied(mut entry) => {
                    let state = entry.get_mut();
                    if state.high_watermark < high {
                        state.high_watermark = high;
                    }
                }
                Entry::Vacant(entry) => {
                    if high > stored_offset {
                        entry.insert(State {
                            high_watermark: high,
                            key_values: HashMap::new(),
                            next_offset: stored_offset,
                        });
                    }
                }
            }
        }

        let last_log = Instant::now();

        let topics_synced_key = topic!(
            ROOT_KEY,
            "applications",
            application,
            "status",
            "allTopicsSynced"
        );

        let synced = topics.is_empty();
        if synced {
            log::info!("No topics are out of sync.");
        } else {
            log::info!("Synchronizing {} topics …", topics.len())
        }

        wb_message_tx
            .send((None, (topics_synced_key.clone(), json!(synced))))
            .into_diagnostic()?;

        Ok(StateAccumulator {
            application,
            transcoder,
            topics,
            wb_message_tx,
            last_log,
            topics_synced_key,
        })
    }

    pub async fn message_arrived(
        &mut self,
        topic: String,
        partition: i32,
        key: Key,
        payload: Option<Vec<u8>>,
        offset: i64,
    ) -> Result<()> {
        if self.last_log.elapsed().as_secs() >= 1 {
            self.last_log = Instant::now();
            self.log_stats();
        }
        match self.topics.entry(topic) {
            Entry::Occupied(mut entry) => {
                let state = entry.get_mut();
                state.key_values.insert(key, (partition, payload));
                state.next_offset = offset + 1;
                if state.high_watermark <= state.next_offset {
                    let (topic, state) = entry.remove_entry();
                    log::info!("Topic '{}' reached high watermark. Accumulated values for {} key(s). Publishing …", topic, state.key_values.len());
                    self.transcode_and_forward_messages(topic, state.key_values, offset)
                        .await?;
                }
                if self.topics.is_empty() {
                    log::info!("All topics synced.");
                    self.wb_message_tx
                        .send((None, (self.topics_synced_key.clone(), json!(true))))
                        .into_diagnostic()?;
                }
            }
            Entry::Vacant(entry) => {
                let topic = entry.into_key();
                self.transcode_and_forward_message(topic, partition, &key, payload, offset)
                    .await?;
            }
        }

        Ok(())
    }

    async fn transcode_and_forward_messages(
        &self,
        topic: String,
        key_values: HashMap<String, (i32, Option<Vec<u8>>)>,
        offset: i64,
    ) -> Result<()> {
        for (key, (partition, value)) in key_values {
            self.transcode_and_forward_message(topic.clone(), partition, &key, value, offset)
                .await?;
        }
        Ok(())
    }

    async fn transcode_and_forward_message(
        &self,
        topic: String,
        partition: i32,
        key: &str,
        payload: Option<Vec<u8>>,
        offset: i64,
    ) -> Result<()> {
        match self.transcoder.transcode(payload).await {
            Ok(value) => {
                let wb_key = topic!(self.application, topic, key);
                self.wb_message_tx
                    .send((Some((topic, partition, offset)), (wb_key, value)))
                    .into_diagnostic()?;
            }
            Err(e) => {
                log::error!(
                    "Error decoding kafka message with offset {offset} of partition {topic}-{partition} : {e}"
                );
            }
        }

        Ok(())
    }

    fn log_stats(&self) {
        for (topic, state) in &self.topics {
            log::info!(
                "Progress of topic '{}': {}/{}; accumulated keys: {}",
                topic,
                state.next_offset,
                state.high_watermark,
                state.key_values.len()
            );
        }
    }
}
