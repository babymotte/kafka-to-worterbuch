use crate::{
    filter::{FilterType, TopicFilter},
    kafka_to_worterbuch, ROOT_KEY,
};
use miette::{IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::select;
use tokio_graceful_shutdown::{NestedSubsystem, SubsystemHandle};
use url::Url;
use worterbuch_client::{topic, KeyValuePair, ServerMessage, StateEvent};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Encoding {
    Avro,
    Json,
    Protobuf,
    PlainText,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub enum Topic {
    Plain(String),
    Filter(TopicFilter),
}

impl Topic {
    pub fn name(&self) -> &str {
        match self {
            Topic::Plain(n) => &n,
            Topic::Filter(f) => &f.name,
        }
    }

    pub fn filter(&self) -> TopicFilter {
        match self {
            Topic::Plain(name) => TopicFilter {
                name: name.clone(),
                set: Some(FilterType::Unconditional(true)),
                publish: None,
                delete: None,
            },
            Topic::Filter(it) => it.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplicationManifest {
    pub bootstrap_servers: Vec<String>,
    pub schema_registry: Option<Url>,
    pub topics: Vec<Topic>,
    pub disabled: Option<bool>,
    #[serde(default = "default_encoding")]
    pub encoding: Encoding,
}

fn default_encoding() -> Encoding {
    Encoding::PlainText
}

struct InstanceManager {
    application: String,
    instances: HashMap<String, NestedSubsystem>,
    subsys: SubsystemHandle,
    proto: String,
    host_addr: String,
    port: u16,
}

impl InstanceManager {
    async fn run(mut self) -> Result<()> {
        let last_will = vec![];
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

        wb.subscribe_unique_async(topic!(
            ROOT_KEY,
            "applications",
            self.application,
            "manifest"
        ))
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
            ServerMessage::State(state) => self.update_instances(state.event).await?,
            ServerMessage::Err(e) => return Err(e).into_diagnostic(),
            _ => (/* ignore */),
        }

        Ok(())
    }

    async fn update_instances(&mut self, state: StateEvent) -> Result<()> {
        match state {
            StateEvent::KeyValue(kvp) => self.spawn_or_update_instance(kvp).await?,
            StateEvent::Deleted(kvp) => self.stop_instance(&kvp.key).await?,
        }

        Ok(())
    }

    async fn spawn_or_update_instance(
        &mut self,
        KeyValuePair { key, value }: KeyValuePair,
    ) -> Result<()> {
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
        Ok(())
    }

    async fn stop_instance(&mut self, key: &str) -> Result<()> {
        if let Some(instance) = self.instances.remove(key) {
            log::info!("Stopping instance for application '{key}'");
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
    application: String,
    proto: String,
    host_addr: String,
    port: u16,
) -> Result<()> {
    log::info!("Starting instance manager …");

    let instances = HashMap::new();

    InstanceManager {
        application,
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
