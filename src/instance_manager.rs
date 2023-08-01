use crate::{kafka_to_worterbuch, ROOT_KEY};
use miette::{IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use tokio::select;
use tokio_graceful_shutdown::{NestedSubsystem, SubsystemHandle};
use url::Url;
use worterbuch_client::{topic, KeyValuePair, PStateEvent, ServerMessage};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Encoding {
    Avro,
    Json,
    Protobuf,
    PlainText,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationManifest {
    pub bootstrap_servers: Vec<String>,
    pub schema_registry: Url,
    pub topics: Vec<String>,
    pub disabled: Option<bool>,
    #[serde(default = "default_encoding")]
    pub encoding: Encoding,
}

fn default_encoding() -> Encoding {
    Encoding::PlainText
}

struct InstanceManager {
    instances: HashMap<String, NestedSubsystem>,
    subsys: SubsystemHandle,
    proto: String,
    host_addr: String,
    port: u16,
}

impl InstanceManager {
    async fn run(mut self) -> Result<()> {
        let last_will_topic = topic!(ROOT_KEY, "status", "running", "instance_manager");

        let last_will = vec![KeyValuePair {
            key: last_will_topic.clone(),
            value: json!(false),
        }];
        let grave_goods = vec![];

        let shutdown = self.subsys.clone();
        let on_disconnect = async move {
            shutdown.request_global_shutdown();
        };

        let mut wb = worterbuch_client::connect(
            &self.proto,
            &self.host_addr,
            self.port,
            last_will,
            grave_goods,
            on_disconnect,
        )
        .await
        .into_diagnostic()?;

        wb.set(last_will_topic, &true).into_diagnostic()?;

        wb.psubscribe_unique_async(topic!(ROOT_KEY, "applications", "?"))
            .into_diagnostic()?;

        let mut messages = wb.responses();

        loop {
            select! {
                recv = messages.recv() => match recv {
                    Ok(msg) => self.process_message(msg).await?,
                    Err(e) => return Err(e).into_diagnostic(),
                },
                _ = self.subsys.on_shutdown_requested() => break,
            }
        }

        wb.close().await;

        Ok(())
    }

    async fn process_message(&mut self, msg: ServerMessage) -> Result<()> {
        match msg {
            ServerMessage::PState(pstate) => self.update_instances(pstate.event).await?,
            ServerMessage::Err(e) => return Err(e).into_diagnostic(),
            ServerMessage::State(_)
            | ServerMessage::Ack(_)
            | ServerMessage::LsState(_)
            | ServerMessage::Handshake(_) => (/* ignore */),
        }

        Ok(())
    }

    async fn update_instances(&mut self, pstate: PStateEvent) -> Result<()> {
        match pstate {
            PStateEvent::KeyValuePairs(kvps) => self.spawn_or_update_instances(kvps).await?,
            PStateEvent::Deleted(kvps) => self.stop_instances(kvps).await?,
        }

        Ok(())
    }

    async fn spawn_or_update_instances(&mut self, kvps: Vec<KeyValuePair>) -> Result<()> {
        for KeyValuePair { key, value } in kvps {
            match serde_json::from_value::<ApplicationManifest>(value) {
                Ok(manifest) => {
                    if let Some(true) = manifest.disabled {
                        self.stop_instance(&key).await?;
                    } else {
                        if self.instances.contains_key(&key) {
                            self.stop_instance(&key).await?;
                        }
                        self.spawn_instance(key, manifest).await?;
                    }
                }
                Err(e) => {
                    log::warn!("could not parse application manifest for '{}': {}", key, e);
                    self.stop_instance(&key).await?;
                }
            }
        }
        Ok(())
    }

    async fn stop_instances(&mut self, kvps: Vec<KeyValuePair>) -> Result<()> {
        for KeyValuePair { key, value: _ } in kvps {
            self.stop_instance(&key).await?;
        }
        Ok(())
    }

    async fn stop_instance(&mut self, application: &str) -> Result<()> {
        if let Some(instance) = self.instances.remove(application) {
            log::info!("Stopping instance for application '{application}'");
            self.subsys.perform_partial_shutdown(instance).await?;
        }

        Ok(())
    }

    async fn spawn_instance(
        &mut self,
        application: String,
        manifest: ApplicationManifest,
    ) -> Result<()> {
        log::info!("Spawning instance for application '{application}'");

        let instance_name = application.clone();

        let proto = self.proto.to_owned();
        let host_addr = self.host_addr.to_owned();
        let port = self.port;

        let instance = self.subsys.start(&instance_name, move |subsys| {
            kafka_to_worterbuch::run(subsys, application, manifest, proto, host_addr, port)
        });

        self.instances.insert(instance_name, instance);
        Ok(())
    }
}

pub async fn run(
    subsys: SubsystemHandle,
    proto: String,
    host_addr: String,
    port: u16,
) -> Result<()> {
    log::info!("Starting instance manager …");

    let instances = HashMap::new();

    InstanceManager {
        host_addr,
        instances,
        proto,
        port,
        subsys,
    }
    .run()
    .await?;

    log::info!("Instance manager stopped.");
    Ok(())
}
