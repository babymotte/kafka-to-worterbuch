use std::time::Duration;

use miette::{IntoDiagnostic, Result};
use rdkafka::{
    config::RDKafkaLogLevel,
    consumer::{Consumer, DefaultConsumerContext, StreamConsumer},
    ClientConfig,
};
use tokio::{runtime, task::spawn_blocking};

pub struct AsyncKafka {
    bootstrap_servers: String,
    group_id: String,
}

const TO: Duration = Duration::from_secs(5);

impl AsyncKafka {
    pub fn new(group_id: String, bootstrap_servers: String) -> Self {
        Self {
            bootstrap_servers,
            group_id,
        }
    }

    pub async fn fetch_watermarks(
        &self,
        topics: Vec<(String, i32)>,
    ) -> Result<Vec<((String, i32), (i64, i64))>> {
        let group_id = self.group_id.clone();
        let bootstrap_servers = self.bootstrap_servers.clone();
        spawn_blocking(move || {
            let runtime = runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .into_diagnostic();
            runtime.and_then(|r| {
                r.block_on(async move {
                    consumer(group_id, bootstrap_servers)
                        .and_then(|c| do_fetch_watermarks(topics, &c))
                })
            })
        })
        .await
        .into_diagnostic()?
    }
}

fn do_fetch_watermarks(
    topics: Vec<(String, i32)>,
    consumer: &StreamConsumer<DefaultConsumerContext>,
) -> Result<Vec<((String, i32), (i64, i64))>> {
    let mut watermarks = Vec::new();

    for (topic, partition) in topics {
        let (low, high) = consumer
            .fetch_watermarks(&topic, partition, TO)
            .into_diagnostic()?;
        watermarks.push(((topic, partition), (low, high)));
    }

    Ok(watermarks)
}

fn consumer(
    group_id: String,
    bootstrap_servers: String,
) -> Result<StreamConsumer<DefaultConsumerContext>> {
    let consumer: StreamConsumer<DefaultConsumerContext> = ClientConfig::new()
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
        .create_with_context(DefaultConsumerContext)
        .into_diagnostic()?;

    Ok(consumer)
}
