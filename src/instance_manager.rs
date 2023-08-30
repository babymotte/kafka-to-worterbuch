use crate::{
    filter::{FilterType, TopicFilter},
    kafka_to_worterbuch, ROOT_KEY,
};
use miette::{IntoDiagnostic, Result};
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio_graceful_shutdown::{NestedSubsystem, SubsystemHandle};
use url::Url;
use worterbuch_client::{config::Config, topic};

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
    instance: Option<NestedSubsystem>,
    subsys: SubsystemHandle,
    wb_config: Config,
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
            self.wb_config.clone(),
            last_will,
            grave_goods,
            on_disconnect,
        )
        .await
        .into_diagnostic()?;

        let (mut manifests, _) = wb
            .subscribe_unique(topic!(
                ROOT_KEY,
                "applications",
                self.application,
                "manifest"
            ))
            .await
            .into_diagnostic()?;

        loop {
            select! {
                recv = manifests.recv() => match recv {
                    Some(msg) => self.process_message(msg).await?,
                    None => self.subsys.request_global_shutdown(),
                },
                _ = self.subsys.on_shutdown_requested() => break,
            }
        }

        wb.close().await.into_diagnostic()?;

        Ok(())
    }

    async fn process_message(&mut self, msg: Option<ApplicationManifest>) -> Result<()> {
        match msg {
            Some(manifest) => self.spawn_or_update_instance(manifest).await?,
            None => self.stop_instance().await?,
        }

        Ok(())
    }

    async fn spawn_or_update_instance(&mut self, manifest: ApplicationManifest) -> Result<()> {
        self.stop_instance().await?;
        match manifest.disabled {
            None | Some(false) => self.spawn_instance(manifest).await?,
            _ => (),
        }
        Ok(())
    }

    async fn stop_instance(&mut self) -> Result<()> {
        if let Some(instance) = self.instance.take() {
            log::info!("Stopping instance for application '{}'", self.application);
            self.subsys.perform_partial_shutdown(instance).await?;
        }

        Ok(())
    }

    async fn spawn_instance(&mut self, manifest: ApplicationManifest) -> Result<()> {
        log::info!("Spawning instance for application '{}'", self.application);

        let config = self.wb_config.clone();

        let application = self.application.clone();

        let instance = self.subsys.start(&self.application, move |subsys| {
            kafka_to_worterbuch::run(subsys, application, manifest, config)
        });

        self.instance = Some(instance);

        Ok(())
    }
}

pub async fn run(subsys: SubsystemHandle, application: String, wb_config: Config) -> Result<()> {
    log::info!("Starting instance manager …");

    let instance = None;

    InstanceManager {
        wb_config,
        application,
        instance,
        subsys,
    }
    .run()
    .await?;

    log::info!("Instance manager stopped.");
    Ok(())
}
