use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{info, debug, error, warn};
use crate::context::AppContext;
use super::ControlPlane;

pub trait ControlPlaneExt {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()>;
}

impl ControlPlaneExt for Arc<AppContext> {
    fn start_control_plane(&self, addr: String) -> tokio::task::JoinHandle<()> {
        let ctx = self.clone();
        let addr = addr.clone();
        let app = ControlPlane::new(ctx.clone()).router();
        let graceful_shutdown = ctx.graceful_shutdown.clone();
        let force_quit = ctx.force_quit.clone();

        tokio::spawn(async move {
            info!("Starting control plane on http://{}", addr);

            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => {
                    debug!("Control plane successfully bound to {}", addr);
                    l
                },
                Err(e) => {
                    error!("Failed to bind control plane to {}: {}", addr, e);
                    return;
                }
            };

            let server = axum::serve(listener, app);

            let shutdown_future = async move {
                tokio::select! {
                    _ = graceful_shutdown.cancelled() => {
                        debug!("Control plane received graceful shutdown signal");
                        sleep(Duration::from_secs(1)).await;
                        debug!("Control plane graceful shutdown completed");
                    }
                    _ = force_quit.cancelled() => {
                        warn!("Control plane received force quit signal");
                    }
                }
            };

            match server.with_graceful_shutdown(shutdown_future).await {
                Ok(_) => debug!("Control plane server shut down successfully"),
                Err(e) => error!("Control plane server error during shutdown: {}", e),
            }
        })
    }
}