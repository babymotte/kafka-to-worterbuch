use crate::{
    client::{K2WbConsumer, K2WbContext},
    filter::{Action, TopicFilter},
    instance_manager::{ApplicationManifest, Topic},
    perf::PerformanceData,
    transcoder::{self, Transcoder},
    ROOT_KEY,
};
use miette::{IntoDiagnostic, Result};
use rdkafka::{
    config::RDKafkaLogLevel, consumer::Consumer, message::BorrowedMessage, ClientConfig, Message,
    Offset,
};
use serde_json::{json, Value};
use std::{collections::HashMap, ops::ControlFlow, time::Duration};
use tokio::{select, sync::mpsc, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;
use worterbuch_client::{topic, KeyValuePair, Worterbuch};
use worterbuch_common::TransactionId;

const TO: Duration = Duration::from_secs(5);

struct KafkaToWorterbuch {
    wb: Worterbuch,
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

        let (post_rebalance_tx, mut post_rebalance_rx) = mpsc::unbounded_channel();
        let context = K2WbContext::new(self.subsys.clone(), post_rebalance_tx);

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

        let mut performance_data = PerformanceData::default();

        let mut acks = self.wb.acks().await.into_diagnostic()?;

        loop {
            select! {
                Some(transaction_id) = acks.recv() => performance_data.message_acked(transaction_id),
                recv = consumer.recv() => match recv {
                    Ok(msg) => {
                        if let Some(transaction_id) = self.process_kafka_message(msg, &transcoder).await? {
                            performance_data.message_queued(transaction_id);
                        }
                        self.track_performance(1, &mut performance_data).await?;
                    },
                    Err(e) => return Err(e).into_diagnostic(),
                },
                _ = post_rebalance_rx.recv() => if let ControlFlow::Break(_) = self.seek_to_stored_offsets(&consumer).await? {
                    break;
                },
                _ = sleep(Duration::from_secs(1)) => self.track_performance(0, &mut performance_data).await?,
                _ = self.subsys.on_shutdown_requested() => break,
            }
        }

        self.wb.close().await.into_diagnostic()?;

        Ok(())
    }

    async fn process_kafka_message(
        &mut self,
        message: BorrowedMessage<'_>,
        transcoder: &impl Transcoder,
    ) -> Result<Option<TransactionId>> {
        match transcoder.transcode(&message).await {
            Ok(value) => self.forward_transcoded_message(message, value).await,
            Err(e) => log::error!(
                "Error decoding kafka message with offset {} from partition {}-{} : {}",
                message.offset(),
                message.topic(),
                message.partition(),
                e
            ),
        }
    }

    async fn forward_transcoded_message(
        &mut self,
        message: BorrowedMessage<'_>,
        value: Value,
    ) -> Result<Option<TransactionId>> {
        let msg_key = decode_key(&message);
        let topic = message.topic();
        let topic_filter = self.topic_filters.get(topic);

        let key = topic!(self.application, topic, msg_key);
        let transaction_id = match topic_filter.and_then(|f| f.apply(&value)) {
            Some(Action::Delete) => Some(self.wb.delete_generic(key).await.into_diagnostic()?.1),
            Some(Action::Publish) => Some(
                self.wb
                    .publish_generic(key, value)
                    .await
                    .into_diagnostic()?,
            ),
            Some(Action::Set) => Some(self.wb.set_generic(key, value).await.into_diagnostic()?),
            None => None,
        };

        self.store_offset(message).await?;

        Ok(transaction_id)
    }

    async fn store_offset(&mut self, message: BorrowedMessage<'_>) -> Result<()> {
        let key = topic!(self.offsets_key, message.topic(), message.partition());
        self.wb
            .set(key, &(message.offset() + 1))
            .await
            .into_diagnostic()?;
        Ok(())
    }

    async fn seek_to_stored_offsets(&mut self, consumer: &K2WbConsumer) -> Result<ControlFlow<()>> {
        log::info!("Seeking to stored offsets …");

        // make sure assignment has completed
        // message can be discarded since we will seek to correct offset afterwards anyway
        select! {
            recv = consumer.recv() => {  recv.into_diagnostic()?; },
            _ = self.subsys.on_shutdown_requested() => return Ok(ControlFlow::Break(())),
        }

        let mut assignment = consumer.assignment().into_diagnostic()?;
        let assigned_partitions: Vec<(String, i32)> = assignment
            .clone()
            .elements()
            .into_iter()
            .map(|e| (e.topic().to_owned(), e.partition()))
            .collect();

        for (topic, partition) in assigned_partitions {
            let offset = self
                .fetch_stored_offset(&topic, partition, consumer)
                .await?;
            log::info!("Seeking  {topic}-{partition}: {offset:?}");
            assignment
                .set_partition_offset(&topic, partition, offset)
                .into_diagnostic()?;
        }
        consumer.seek_partitions(assignment, TO).into_diagnostic()?;

        log::info!("Done. Ready to forward messages.");

        Ok(ControlFlow::Continue(()))
    }

    async fn fetch_stored_offset(
        &mut self,
        topic: &str,
        partition: i32,
        consumer: &K2WbConsumer,
    ) -> Result<Offset> {
        let (low_watermark, high_watermark) = consumer
            .fetch_watermarks(&topic, partition, TO)
            .into_diagnostic()?;
        Ok(self
            .wb
            .get::<i64>(topic!(self.offsets_key, topic, partition))
            .await
            .into_diagnostic()?
            .0;
        log::debug!("Got offset from worterbuch: {offset:?}");
        let offset = offset
            .map(|o| Offset::Offset(o.max(low_watermark).min(high_watermark)))
            .unwrap_or(Offset::Beginning);
        log::debug!("Using offset {offset:?}.");
        Ok(offset)
    }

    async fn track_performance(
        &mut self,
        msg_count: u64,
        performance_data: &mut PerformanceData,
    ) -> Result<()> {
        if let Some(data) = performance_data.update(msg_count) {
            self.publish_performance_data(data).await?;
        }
        Ok(())
    }

    async fn publish_performance_data(
        &mut self,
        (mps, mpm, mph, inflght): (u64, u64, u64, usize),
    ) -> Result<()> {
        self.wb
            .set(
                topic!(
                    ROOT_KEY,
                    "applications",
                    self.application,
                    "status",
                    "messagesPerSecond"
                ),
                &mps,
            )
            .await
            .into_diagnostic()?;
        self.wb
            .set(
                topic!(
                    ROOT_KEY,
                    "applications",
                    self.application,
                    "status",
                    "messagesPerMinute"
                ),
                &mpm,
            )
            .await
            .into_diagnostic()?;
        self.wb
            .set(
                topic!(
                    ROOT_KEY,
                    "applications",
                    self.application,
                    "status",
                    "messagesPerHour"
                ),
                &mph,
            )
            .await
            .into_diagnostic()?;
        self.wb
            .set(
                topic!(
                    ROOT_KEY,
                    "applications",
                    self.application,
                    "status",
                    "inFlightMessages"
                ),
                &inflght,
            )
            .await
            .into_diagnostic()?;
        Ok(())
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

    let last_will_running_topic =
        topic!(ROOT_KEY, "applications", application, "status", "running");
    let last_will_msg_rate_topic = topic!(
        ROOT_KEY,
        "applications",
        application,
        "status",
        "messagesPerSecond"
    );

    let last_will = vec![
        KeyValuePair {
            key: last_will_running_topic.clone(),
            value: json!(false),
        },
        KeyValuePair {
            key: last_will_msg_rate_topic.clone(),
            value: json!(0),
        },
    ];
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

    wb.set(last_will_running_topic, &true)
        .await
        .into_diagnostic()?;

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
