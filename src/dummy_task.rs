use miette::Result;
use std::time::Duration;
use tokio::{select, time::sleep};
use tokio_graceful_shutdown::SubsystemHandle;

pub async fn dummy_task(subsys: SubsystemHandle) -> Result<()> {
    log::info!("dummy_task started.");

    loop {
        select! {
            _ = sleep(Duration::from_secs(1)) => log::info!("ping"),
            _ = subsys.on_shutdown_requested() => break,
        }
    }

    log::info!("dummy_task stopped");

    Ok(())
}
