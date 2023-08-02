use crate::{
    client::{K2WbConsumer, K2WbContext},
    filter::{Action, TopicFilter},
    instance_manager::{ApplicationManifest, Topic},
    transcoder::{self, Transcoder},
    ROOT_KEY,
};
use miette::{IntoDiagnostic, Result};
use rdkafka::{
    config::RDKafkaLogLevel, consumer::Consumer, message::BorrowedMessage, ClientConfig, Message,
    Offset,
};
use serde_json::{json, Value};
use std::{collections::HashMap, time::Duration};
use tokio::{select, sync::mpsc};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_client::{topic, Connection, KeyValuePair};

const TO: Duration = Duration::from_secs(5);

struct KafkaToWorterbuch {
    wb: Connection,
    subsys: SubsystemHandle,
    manifest: ApplicationManifest,
    application: String,
    topic_filters: HashMap<String, TopicFilter>,
    offsets_key: String,
}

impl KafkaToWorterbuch {
    async fn run(mut self) -> Result<()> {
        let group_id = format!("kafka-to-worterbuch-{}", self.application);
        let bootstrap_servers = self.manifest.bootstrap_servers.join(",");

        let (rebalance_tx, mut rebalance_rx) = mpsc::unbounded_channel();

        let context = K2WbContext::new(self.subsys.clone(), rebalance_tx);

        let consumer: K2WbConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("statistics.interval.ms", "0")
            .set("fetch.error.backoff.ms", "1")
            .set("auto.offset.reset", "beginning")
            .set("socket.nagle.disable", "true")
            .set("allow.auto.create.topics", "false")
            .set_log_level(RDKafkaLogLevel::Warning)
            .create_with_context(context)
            .into_diagnostic()?;

        let topics: Vec<&str> = self.manifest.topics.iter().map(Topic::name).collect();

        log::info!("Subscribing to topics: {topics:?} …");
        consumer.subscribe(&topics).into_diagnostic()?;
        log::info!("Subscription done. Waiting for rebalance …");

        let transcoder = transcoder::transcoder_for(&self.manifest)?;

        loop {
            select! {
                recv = consumer.recv() => match recv {
                    Ok(msg) => self.process_kafka_message(msg, &transcoder).await?,
                    Err(e) => return Err(e).into_diagnostic(),
                },
                assignment = rebalance_rx.recv() => match assignment {
                    Some(assignment) => self.seek_to_stored_offsets(assignment, &consumer).await?,
                    None => break,
                },
                _ = self.subsys.on_shutdown_requested() => break,
            }
        }

        Ok(())
    }

    async fn process_kafka_message(
        &mut self,
        message: BorrowedMessage<'_>,
        transcoder: &impl Transcoder,
    ) -> Result<()> {
        match transcoder.transcode(&message).await {
            Ok(value) => self.forward_transcoded_message(message, value)?,
            Err(e) => log::error!("Error decoding kafka message: {e}"),
        }

        Ok(())
    }

    fn forward_transcoded_message(
        &mut self,
        message: BorrowedMessage<'_>,
        value: Value,
    ) -> Result<()> {
        let msg_key = decode_key(&message);
        let topic = message.topic();
        let topic_filter = self.topic_filters.get(topic);

        let key = topic!(self.application, topic, msg_key);
        match topic_filter.and_then(|f| f.apply(&value)) {
            Some(Action::Delete) => self.wb.delete_async(key),
            Some(Action::Publish) => self.wb.publish(key, &value),
            _ => self.wb.set(key, &value),
        }
        .into_diagnostic()?;

        self.store_offset(message)?;

        Ok(())
    }

    fn store_offset(&mut self, message: BorrowedMessage<'_>) -> Result<()> {
        let key = topic!(self.offsets_key, message.topic(), message.partition());
        self.wb.set(key, &message.offset()).into_diagnostic()?;
        Ok(())
    }

    async fn seek_to_stored_offsets(
        &mut self,
        assignment: Vec<(String, i32)>,
        consumer: &K2WbConsumer,
    ) -> Result<()> {
        log::info!("Rebalance complete; assigned: {:?}", assignment);

        log::info!("Seeking to stored offsets …");

        for (topic, partition) in assignment {
            self.seek_to_stored_offset(topic, partition, consumer)
                .await?;
        }

        log::info!("Done. Ready to forward messages.");

        Ok(())
    }

    async fn seek_to_stored_offset(
        &mut self,
        topic: String,
        partition: i32,
        consumer: &K2WbConsumer,
    ) -> Result<()> {
        let offset = self.fetch_stored_offset(&topic, partition).await;
        consumer
            .seek(&topic, partition, offset, TO)
            .into_diagnostic()?;
        Ok(())
    }

    async fn fetch_stored_offset(&mut self, topic: &String, partition: i32) -> Offset {
        self.wb
            .get::<i64>(topic!(self.offsets_key, topic, partition))
            .await
            .ok()
            .map(|o| Offset::Offset(o + 1))
            .unwrap_or(Offset::Beginning)
    }
}

fn decode_key(message: &BorrowedMessage<'_>) -> String {
    message
        .key()
        .map(String::from_utf8_lossy)
        .map(|s| s.to_string())
        .unwrap_or_else(|| "<no_key>".to_owned())
}

pub async fn run(
    subsys: SubsystemHandle,
    application: String,
    mut manifest: ApplicationManifest,
    proto: String,
    host_addr: String,
    port: u16,
) -> Result<()> {
    let application = application
        .split('/')
        .skip(2)
        .next()
        .map(ToOwned::to_owned)
        .unwrap_or(application);

    log::info!("Starting '{application}' …");

    let last_will_topic = topic!(ROOT_KEY, "status", "running", application);

    let last_will = vec![KeyValuePair {
        key: last_will_topic.clone(),
        value: json!(false),
    }];
    let grave_goods = vec![];

    let shutdown = subsys.clone();
    let on_disconnect = async move {
        shutdown.request_global_shutdown();
    };

    let mut wb = worterbuch_client::connect(
        &proto,
        &host_addr,
        port,
        last_will,
        grave_goods,
        on_disconnect,
    )
    .await
    .into_diagnostic()?;

    wb.set(last_will_topic, &true).into_diagnostic()?;

    let stopped_application = application.clone();
    let topic_filters = build_topic_filters(&mut manifest);
    let offsets_key = topic!(ROOT_KEY, "applications", application, "offsets");

    KafkaToWorterbuch {
        manifest,
        subsys,
        wb,
        application,
        topic_filters,
        offsets_key,
    }
    .run()
    .await?;

    log::info!("'{stopped_application}' stopped.");

    Ok(())
}

fn build_topic_filters(manifest: &mut ApplicationManifest) -> HashMap<String, TopicFilter> {
    let mut map = HashMap::new();

    for topic in manifest.topics.iter_mut() {
        let topic_name = topic.name().to_owned();
        let topic_filter = topic.filter();
        map.insert(topic_name, topic_filter);
    }

    map
}
