use crate::context::AppContext;
use sea_orm::*;
use sqlx::postgres::PgListener;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// PostgreSQL LISTEN/NOTIFY manager for reducing queue latency
pub struct NotifyManager {
    ctx: Arc<AppContext>,
    channel_name: String,
}



impl NotifyManager {
    pub fn new(ctx: Arc<AppContext>, queue_name: &str) -> Self {
        let channel_name = format!("solid_queue_{}", queue_name);
        Self {
            ctx,
            channel_name,
        }
    }

    /// Start listening for PostgreSQL NOTIFY messages
    /// Returns a receiver that will get notifications when new jobs are available
    pub async fn start_listener(&self) -> Result<mpsc::Receiver<String>, anyhow::Error> {
        if !self.ctx.is_postgres() {
            return Err(anyhow::anyhow!("LISTEN/NOTIFY is only supported on PostgreSQL"));
        }

        let (tx, rx) = mpsc::channel::<String>(200);
        let channel = self.channel_name.clone();
        let dsn = self.ctx.dsn.to_string();
        let graceful_shutdown = self.ctx.graceful_shutdown.clone();

        // Spawn a background task to handle LISTEN using sqlx
        tokio::spawn(async move {
            let mut retry_count = 0;
            const MAX_RETRIES: u32 = 5;

            loop {
                if graceful_shutdown.is_cancelled() {
                    info!("LISTEN task for {} shutting down", channel);
                    break;
                }

                match Self::listen_loop(&dsn, &channel, &tx, &graceful_shutdown).await {
                    Ok(_) => {
                        info!("LISTEN loop for {} completed normally", channel);
                        break;
                    }
                    Err(e) => {
                        retry_count += 1;
                        error!("LISTEN error for {}: {} (attempt {}/{})", channel, e, retry_count, MAX_RETRIES);

                        if retry_count >= MAX_RETRIES {
                            error!("Max retries reached for LISTEN on {}, giving up", channel);
                            break;
                        }

                        // Exponential backoff
                        let delay = std::time::Duration::from_secs(2_u64.pow(retry_count.min(5)));
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        });

        Ok(rx)
    }

    /// LISTEN loop implementation using sqlx PgListener
    async fn listen_loop(
        dsn: &str,
        channel: &str,
        tx: &mpsc::Sender<String>,
        graceful_shutdown: &tokio_util::sync::CancellationToken,
    ) -> Result<(), anyhow::Error> {
        // Create a dedicated connection for LISTEN with optimized settings
        // Note: PgListener creates its own connection internally, so we use the DSN directly
        let mut listener = PgListener::connect(dsn).await?;

        // Start listening on the channel
        listener.listen(channel).await?;
        info!("Started LISTEN on channel: {} (dedicated connection)", channel);

        loop {
            tokio::select! {
                // Wait for NOTIFY messages
                notification = listener.recv() => {
                    match notification {
                        Ok(notification) => {
                            trace!("Received NOTIFY on {}: {}", notification.channel(), notification.payload());

                            // Use try_send to avoid blocking - if channel is full, drop the notification
                            // This implements natural backpressure: when worker can't keep up, skip notifications
                            match tx.try_send(notification.payload().to_string()) {
                                Ok(_) => {
                                    trace!("NOTIFY message sent to worker queue");
                                }
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!("NOTIFY channel full - dropping notification (worker overloaded)");
                                    // Don't break the loop, just drop this notification and continue
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    warn!("NOTIFY channel closed - stopping LISTEN loop");
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error receiving NOTIFY on {}: {}", channel, e);
                            return Err(anyhow::anyhow!("LISTEN receive error: {}", e));
                        }
                    }
                }
                // Check for shutdown signal
                _ = graceful_shutdown.cancelled() => {
                    info!("LISTEN loop for {} cancelled by shutdown signal", channel);
                    break;
                }
            }
        }

        // Clean up listener (automatically unlistens when dropped)
        info!("Stopped LISTEN on channel: {}", channel);
        Ok(())
    }

    /// Send a NOTIFY message (used by the job enqueue process)
    pub async fn notify(&self, message: &str) -> Result<(), anyhow::Error> {
        if !self.ctx.is_postgres() {
            trace!("Skipping NOTIFY - not using PostgreSQL");
            return Ok(());
        }

        let db = self.ctx.get_db().await;
        Self::send_notify_with_db(&*db, &self.channel_name, message).await
    }

    /// Static method to send NOTIFY using existing database connection
    /// This is more efficient for one-off notifications
    pub async fn send_notify<C>(db: &C, queue_name: &str, message: &str) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        let channel_name = format!("solid_queue_{}", queue_name);
        Self::send_notify_with_db(db, &channel_name, message).await
    }

    /// Internal helper to send NOTIFY with database connection and channel name
    async fn send_notify_with_db<C>(db: &C, channel_name: &str, message: &str) -> Result<(), anyhow::Error>
    where
        C: ConnectionTrait,
    {
        // PostgreSQL NOTIFY doesn't support parameterized queries, so we need to escape and format directly
        let escaped_message = message.replace("'", "''"); // Escape single quotes
        let sql = format!("NOTIFY {}, '{}'", channel_name, escaped_message);

        let ret = db
            .execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                sql,
                vec![],
            ))
            .await;

        match ret {
            Ok(_) => {
                trace!("NOTIFY sent on channel {}: {}", channel_name, message);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to send NOTIFY on channel {}: {}", channel_name, e);
                Err(anyhow::anyhow!("NOTIFY failed: {}", e))
            }
        }
    }

    /// Get the channel name this manager is listening on
    pub fn get_channel_name(&self) -> &str {
        &self.channel_name
    }
}

/// Helper function to create a NotifyManager for a specific queue
pub fn create_notify_manager(ctx: Arc<AppContext>, queue_name: &str) -> NotifyManager {
    NotifyManager::new(ctx, queue_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_channel_name_generation() {
        let dsn = Url::parse("postgres://user:pass@localhost/test").expect("Valid test URL");
        let ctx = Arc::new(AppContext::new(dsn, None, sea_orm::ConnectOptions::new("test".to_string()), None));
        let manager = NotifyManager::new(ctx, "default");
        assert_eq!(manager.get_channel_name(), "solid_queue_default");
    }

    #[test]
    fn test_is_postgres_detection() {
        let postgres_dsn = Url::parse("postgres://user:pass@localhost/test").expect("Valid postgres test URL");
        let postgres_ctx = Arc::new(AppContext::new(postgres_dsn, None, sea_orm::ConnectOptions::new("test".to_string()), None));
        assert!(postgres_ctx.is_postgres());

        let sqlite_dsn = Url::parse("sqlite://test.db").expect("Valid sqlite test URL");
        let sqlite_ctx = Arc::new(AppContext::new(sqlite_dsn, None, sea_orm::ConnectOptions::new("test".to_string()), None));
        assert!(!sqlite_ctx.is_postgres());
    }
}
