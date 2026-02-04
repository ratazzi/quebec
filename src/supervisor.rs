// TODO: Supervisor is a placeholder for future process-based architecture.
// Currently Quebec uses a thread-based model. When migrating to multi-process
// mode (like Solid Queue's fork-based supervision), this module will manage
// child process lifecycles, health checks, and restarts.
#![allow(dead_code)]

use crate::context::AppContext;
use crate::process::{ProcessInfo, ProcessTrait};
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
        let mut heartbeat_interval = tokio::time::interval(self.ctx.process_heartbeat_interval);

        loop {
            tokio::select! {
              _ = heartbeat_interval.tick() => {}
              _ = tokio::signal::ctrl_c() => {
                info!("ctrl-c received");
                return Ok(());
              }
            }
        }
    }
}

#[async_trait]
impl ProcessTrait for Supervisor {
    fn ctx(&self) -> &Arc<AppContext> {
        &self.ctx
    }

    fn process_info(&self) -> ProcessInfo {
        ProcessInfo::new("Supervisor", "supervisor")
    }
}
