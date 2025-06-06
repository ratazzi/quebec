use crate::context::AppContext;

use crate::process::ProcessTrait;
use anyhow::Result;
use async_trait::async_trait;

use std::sync::Arc;

use tracing::info;

#[derive(Debug)]
pub struct Supervisor {
    pub ctx: Arc<AppContext>,
}

impl Supervisor {
    pub fn new(ctx: Arc<AppContext>) -> Self {
        Self { ctx }
    }

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        // let db = self.ctx.get_db().await?;
        // let mut polling_interval = tokio::time::interval(self.ctx.supervisor_polling_interval);
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);
        // let batch_size = self.ctx.supervisor_batch_size;

        // let kind = "Supervisor".to_string();
        // let name = "supervisor".to_string();
        // let process = self.on_start(&db, kind, name).await?;
        // info!(">> Process started: {:?}", process);

        loop {
            tokio::select! {
              _ = heartbeat_interval.tick() => {
                // let process = self.on_heartbeat(&db, process.clone()).await?;
                // info!(">> Process heartbeat: {:?}", process);
              }
              _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received");
                return Ok(());
              }
              // _ = polling_interval.tick() => {
              //   let scheduled_entries = self.get_scheduled_entries(&db, batch_size).await?;
              //   info!(">> Scheduled entries: {:?}", scheduled_entries);
              //   self.on_poll(&db, scheduled_entries).await?;
              // }
            }
        }
    }
}

#[async_trait]
impl ProcessTrait for Supervisor {}
