use crate::{
    instance_manager::ApplicationManifest,
    transcoder::{self, Transcoder},
    ROOT_KEY,
};
use miette::{IntoDiagnostic, Result};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message, TopicPartitionList,
};
use regex::Regex;
use serde_json::json;
use std::time::Duration;
use tokio::{select, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_client::{topic, Connection};
use worterbuch_common::KeyValuePair;

struct KafkaToWorterbuch {
    wb: Connection,
    subsys: SubsystemHandle,
    manifest: ApplicationManifest,
    application: String,
    topic_regex: Regex,
}

pub struct K2WbContext {}

impl ClientContext for K2WbContext {}

impl K2WbContext {
    pub fn new() -> Self {
        K2WbContext {}
    }
}

impl ConsumerContext for K2WbContext {
    fn pre_rebalance(&self, _rebalance: &Rebalance) {
        log::info!("Starting rebalance …");
    }

    fn post_rebalance(&self, _rebalance: &Rebalance) {
        log::info!("Rebalance complete.");
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        log::debug!("Committing offsets: {:?}", result);
    }
}

pub type K2WbConsumer = StreamConsumer<K2WbContext>;

impl KafkaToWorterbuch {
    async fn run(mut self) -> Result<()> {
        let group_id = format!("kafka-to-worterbuch-{}", self.application);
        let bootstrap_servers = self.manifest.bootstrap_servers.join(",");

        let context = K2WbContext::new();

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

        let topics: Vec<&str> = self.manifest.topics.iter().map(String::as_str).collect();

        log::info!("Subscribing to topics: {topics:?} …");

        consumer.subscribe(&topics).into_diagnostic()?;

        log::info!("Subscription done. Waiting for rebalance …");

        let mut msg_received = false;

        loop {
            select! {
                recv = consumer.recv() => match recv {
                    Ok(msg) => {
                        msg_received = true;
                        consumer.store_offset_from_message(&msg).into_diagnostic()?;
                        self.process_kafka_message(msg).await?;
                    },
                    Err(e) => return Err(e).into_diagnostic(),
                },
                _ = sleep(Duration::from_secs(5)) => {
                    if msg_received {
                        self.commit_offsets(&consumer)?;
                        msg_received = false;
                    }
                },
                _ = self.subsys.on_shutdown_requested() => break,
            }
        }

        Ok(())
    }

    async fn process_kafka_message(&mut self, message: BorrowedMessage<'_>) -> Result<()> {
        match transcoder::transcoder_for(&self.manifest.encoding).transcode(&message) {
            Ok(value) => {
                let key = message
                    .key()
                    .map(String::from_utf8_lossy)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "<no_key>".to_owned());
                let mut topic = message.topic().to_owned();
                if !self.topic_regex.is_match(&topic) {
                    topic = format!("{topic}_v1");
                }
                let key = topic!(self.application, topic, key);
                self.wb.set(key, &value).into_diagnostic()?;
            }
            Err(e) => log::error!("Error decoding kafka message: {e}"),
        }

        Ok(())
    }

    fn commit_offsets(&self, consumer: &K2WbConsumer) -> Result<()> {
        consumer
            .commit_consumer_state(CommitMode::Async)
            .into_diagnostic()
    }
}

pub async fn run(
    subsys: SubsystemHandle,
    application: String,
    manifest: ApplicationManifest,
    proto: String,
    host_addr: String,
    port: u16,
) -> Result<()> {
    let application = application
        .split('/')
        .last()
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

    let topic_regex: Regex = Regex::new(r"(_v[0-9]+)$").into_diagnostic()?;

    KafkaToWorterbuch {
        manifest,
        subsys,
        wb,
        application,
        topic_regex,
    }
    .run()
    .await?;

    log::info!("'{stopped_application}' stopped.");

    Ok(())
}
