use crate::context::AppContext;
use crate::error::{QuebecError, Result};
use sea_orm::*;
use sqlx::postgres::PgListener;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info, trace, warn};

/// Per-queue last-NOTIFY timestamps, shared across the process. Producers
/// consult this table before emitting a NOTIFY to coalesce bursts (e.g.
/// thousands of `perform_later` calls in a loop) into one wake-up per
/// `notify_throttle_interval`. The window is per process — workers still
/// catch up via polling / IDLE fallback if a NOTIFY is dropped.
static LAST_NOTIFY: LazyLock<Mutex<HashMap<String, Instant>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Decide whether a NOTIFY for `queue` should be emitted now.
///
/// Returns `true` and records the emission time when the queue has not been
/// notified within the last `throttle`. Returns `false` when an earlier
/// NOTIFY is still inside the throttle window. `throttle == Duration::ZERO`
/// disables throttling and always returns `true` (no state recorded).
pub fn should_emit_notify(queue: &str, throttle: Duration) -> bool {
    if throttle.is_zero() {
        return true;
    }
    let now = Instant::now();
    let mut map = LAST_NOTIFY.lock().expect("notify throttle map poisoned");
    match map.get(queue) {
        Some(last) if now.duration_since(*last) < throttle => false,
        _ => {
            map.insert(queue.to_string(), now);
            true
        }
    }
}

#[cfg(test)]
pub(crate) fn reset_notify_throttle() {
    LAST_NOTIFY
        .lock()
        .expect("notify throttle map poisoned")
        .clear();
}

/// PostgreSQL LISTEN/NOTIFY manager for reducing queue latency
pub struct NotifyManager {
    ctx: Arc<AppContext>,
    channel_name: String,
}

/// Notification message structure
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NotifyMessage {
    pub queue: String,
    pub event: String,
}

impl NotifyManager {
    /// Create a new NotifyManager for the global jobs channel
    /// Channel name format: {app_name}_jobs (e.g., "quebec_jobs" or "myapp_jobs")
    pub fn new(ctx: Arc<AppContext>) -> Self {
        let channel_name = format!("{}_jobs", ctx.name);
        Self { ctx, channel_name }
    }

    /// Start listening for PostgreSQL NOTIFY messages
    /// Returns a receiver that will get notifications when new jobs are available
    pub async fn start_listener(&self) -> Result<mpsc::Receiver<String>> {
        if !self.ctx.is_postgres() {
            return Err(QuebecError::Unsupported(
                "LISTEN/NOTIFY is only supported on PostgreSQL".into(),
            ));
        }

        let (tx, rx) = mpsc::channel::<String>(200);
        let channel = self.channel_name.clone();
        let dsn = self.ctx.dsn.as_connect_str().to_string();
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
                        error!(
                            "LISTEN error for {}: {} (attempt {}/{})",
                            channel, e, retry_count, MAX_RETRIES
                        );

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
    ) -> Result<()> {
        // Create a dedicated connection for LISTEN with optimized settings
        // Note: PgListener creates its own connection internally, so we use the DSN directly
        let mut listener = PgListener::connect(dsn)
            .await
            .map_err(|e| QuebecError::Other(e.into()))?;

        // Start listening on the channel
        listener
            .listen(channel)
            .await
            .map_err(|e| QuebecError::Other(e.into()))?;
        info!(
            "Started LISTEN on channel: {} (dedicated connection)",
            channel
        );

        loop {
            tokio::select! {
                notification = listener.recv() => {
                    let notification = match notification {
                        Ok(n) => n,
                        Err(e) => {
                            error!("Error receiving NOTIFY on {}: {}", channel, e);
                            return Err(QuebecError::Other(e.into()));
                        }
                    };

                    trace!("Received NOTIFY on {}: {}", notification.channel(), notification.payload());

                    // Use try_send to avoid blocking - if channel is full, drop the notification
                    // This implements natural backpressure: when worker can't keep up, skip notifications
                    match tx.try_send(notification.payload().to_string()) {
                        Ok(_) => trace!("NOTIFY message sent to worker queue"),
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!("NOTIFY channel full - dropping notification (worker overloaded)");
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            warn!("NOTIFY channel closed - stopping LISTEN loop");
                            break;
                        }
                    }
                }
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

    /// Static method to send NOTIFY using existing database connection
    pub async fn send_notify<C>(app_name: &str, db: &C, queue_name: &str, event: &str) -> Result<()>
    where
        C: ConnectionTrait,
    {
        let message = NotifyMessage {
            queue: queue_name.to_string(),
            event: event.to_string(),
        };
        let message_json = serde_json::to_string(&message)?;

        // Use the same naming convention as LISTEN
        let channel_name = format!("{app_name}_jobs");
        Self::send_notify_with_db(db, &channel_name, &message_json).await
    }

    /// Internal helper to send NOTIFY with database connection and channel name
    async fn send_notify_with_db<C>(db: &C, channel_name: &str, message: &str) -> Result<()>
    where
        C: ConnectionTrait,
    {
        // PostgreSQL NOTIFY doesn't support parameterized queries, so we need to escape and format directly
        let escaped_message = message.replace("'", "''"); // Escape single quotes
        let sql = format!("NOTIFY {channel_name}, '{escaped_message}'");

        match db
            .execute(Statement::from_sql_and_values(
                db.get_database_backend(),
                sql,
                vec![],
            ))
            .await
        {
            Ok(_) => {
                trace!("NOTIFY sent on channel {}: {}", channel_name, message);
                Ok(())
            }
            Err(e) => {
                warn!("Failed to send NOTIFY on channel {}: {}", channel_name, e);
                Err(e.into())
            }
        }
    }

    /// Get the channel name this manager is listening on
    #[cfg(test)]
    pub fn get_channel_name(&self) -> &str {
        &self.channel_name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_url::DatabaseUrl;

    /// Helper to create AppContext for tests (handles cfg-gated signature)
    fn make_ctx(dsn: DatabaseUrl) -> Arc<AppContext> {
        #[cfg(feature = "python")]
        {
            Arc::new(AppContext::new(
                dsn,
                None,
                sea_orm::ConnectOptions::new("test".to_string()),
                None,
            ))
        }
        #[cfg(not(feature = "python"))]
        {
            Arc::new(AppContext::new(
                dsn,
                None,
                sea_orm::ConnectOptions::new("test".to_string()),
            ))
        }
    }

    #[test]
    fn test_channel_name_generation() {
        let dsn =
            DatabaseUrl::parse("postgres://user:pass@localhost/test").expect("Valid test URL");
        let ctx = make_ctx(dsn);
        let manager = NotifyManager::new(ctx);
        assert_eq!(manager.get_channel_name(), "quebec_jobs");
    }

    #[test]
    fn test_is_postgres_detection() {
        let postgres_dsn = DatabaseUrl::parse("postgres://user:pass@localhost/test")
            .expect("Valid postgres test URL");
        let postgres_ctx = make_ctx(postgres_dsn);
        assert!(postgres_ctx.is_postgres());

        let sqlite_dsn = DatabaseUrl::parse("sqlite://test.db").expect("Valid sqlite test URL");
        let sqlite_ctx = make_ctx(sqlite_dsn);
        assert!(!sqlite_ctx.is_postgres());
    }

    #[test]
    fn test_throttle_zero_disables() {
        reset_notify_throttle();
        for _ in 0..5 {
            assert!(should_emit_notify("zero", Duration::ZERO));
        }
    }

    #[test]
    fn test_throttle_drops_within_window() {
        reset_notify_throttle();
        let throttle = Duration::from_secs(60);
        assert!(should_emit_notify("within", throttle));
        // Subsequent calls inside the window are dropped.
        for _ in 0..10 {
            assert!(!should_emit_notify("within", throttle));
        }
    }

    #[test]
    fn test_throttle_allows_after_window() {
        reset_notify_throttle();
        let throttle = Duration::from_millis(10);
        assert!(should_emit_notify("after", throttle));
        std::thread::sleep(Duration::from_millis(20));
        assert!(should_emit_notify("after", throttle));
    }

    #[test]
    fn test_throttle_is_per_queue() {
        reset_notify_throttle();
        let throttle = Duration::from_secs(60);
        assert!(should_emit_notify("queue_a", throttle));
        assert!(should_emit_notify("queue_b", throttle));
        assert!(!should_emit_notify("queue_a", throttle));
        assert!(!should_emit_notify("queue_b", throttle));
    }
}
