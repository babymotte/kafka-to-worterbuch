use rdkafka::{
    consumer::{ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
    ClientContext, TopicPartitionList,
};
use tokio::sync::mpsc;
use tokio_graceful_shutdown::SubsystemHandle;

pub struct K2WbContext {
    subsys: SubsystemHandle,
    rebalance_tx: mpsc::UnboundedSender<Vec<(String, i32)>>,
}

impl ClientContext for K2WbContext {}

impl K2WbContext {
    pub fn new(
        subsys: SubsystemHandle,
        rebalance_tx: mpsc::UnboundedSender<Vec<(String, i32)>>,
    ) -> Self {
        K2WbContext {
            subsys,
            rebalance_tx,
        }
    }
}

impl ConsumerContext for K2WbContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(ass) => {
                log::info!(
                    "Starting rebalance; assigned: {:?}",
                    ass.to_topic_map()
                        .into_keys()
                        .collect::<Vec<(String, i32)>>()
                )
            }
            Rebalance::Revoke(rev) => {
                log::info!(
                    "Starting rebalance; assignment revoked: {:?}",
                    rev.to_topic_map()
                        .into_keys()
                        .collect::<Vec<(String, i32)>>()
                )
            }
            Rebalance::Error(err) => {
                log::error!("Rebalance error: {err}");
                self.subsys.request_global_shutdown();
            }
        }
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(ass) => {
                let partitions = ass.to_topic_map().into_keys().collect();
                if let Err(e) = self.rebalance_tx.send(partitions) {
                    log::error!("Could not notify client of assigned partitions: {e}");
                    self.subsys.request_global_shutdown();
                }
            }
            Rebalance::Revoke(rev) => {
                log::info!(
                    "Rebalance complete; assignment revoked: {:?}",
                    rev.to_topic_map()
                        .into_keys()
                        .collect::<Vec<(String, i32)>>()
                )
            }
            Rebalance::Error(err) => {
                log::error!("Rebalance error: {err}");
                self.subsys.request_global_shutdown();
            }
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        match result {
            Ok(_) => log::debug!(
                "Offsets committed: {:?}",
                offsets
                    .to_topic_map()
                    .keys()
                    .map(|(topic, part)| format!("{topic}-{part}"))
                    .collect::<Vec<String>>()
            ),
            Err(e) => {
                log::error!("Error committing offsets: {e}");
                self.subsys.request_global_shutdown();
            }
        }
    }
}

pub type K2WbConsumer = StreamConsumer<K2WbContext>;
