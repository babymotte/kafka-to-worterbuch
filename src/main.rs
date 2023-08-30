mod accumulator;
mod async_kafka;
mod client;
mod command_line;
mod filter;
mod instance_manager;
mod kafka_to_worterbuch;
mod perf;
mod transcoder;

use miette::{miette, Result};
use tokio::time::Duration;
use tokio_graceful_shutdown::Toplevel;
use worterbuch_client::config::Config;

const ROOT_KEY: &str = "kafka-to-worterbuch";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let opts = command_line::parse();

    let application = opts.application;

    if let (Some(tls), Some(addr), Some(port)) = (opts.tls, opts.address, opts.port) {
        let proto = (if tls { "wss" } else { "ws" }).to_owned();

        log::info!("Using worterbuch server {proto}://{addr}:{port}/ws");

        let wb_config = Config::with_address(proto, addr, port);

        Toplevel::new()
            .start("instance_manager", move |subsys| {
                instance_manager::run(subsys, application, wb_config)
            })
            .catch_signals()
            .handle_shutdown_requests(Duration::from_millis(1000))
            .await
            .map_err(Into::into)
    } else {
        return Err(miette!("no worterbuch server specified"));
    }
}
