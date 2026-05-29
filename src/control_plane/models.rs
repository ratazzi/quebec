use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct QueueInfo {
    pub name: String,
    /// HTML-id-safe version of `name` used as the anchor for turbo-stream
    /// updates. Non `[A-Za-z0-9_-]` characters are replaced with `_`.
    pub slug: String,
    pub jobs_count: i64,
    pub status: String,
    /// EXPERIMENTAL queue-level concurrency limit (from
    /// `experimental_queue_concurrency`). `None` means no global cap is in
    /// effect for this queue. Surfaced in the queues page so operators can
    /// see at a glance which queues are currently throttled and to what
    /// level.
    pub concurrency_limit: Option<i32>,
}

/// Encode a queue name into an HTML-id-safe slug with **no collisions**.
/// Used to anchor per-queue turbo-stream targets (`queue-count-{slug}`,
/// `queue-status-{slug}`).
///
/// Standard URL percent-encoding semantics, byte-by-byte: `[A-Za-z0-9_-]`
/// pass through unchanged, everything else becomes `%HH` (uppercase hex of
/// the UTF-8 byte). Guarantees a one-to-one mapping — a naive "replace
/// with `_`" strategy would collapse `a.b`, `a/b`, `a b` onto the same DOM
/// element and silently freeze all but the first one's live counter.
///
/// `%` in an HTML `id` attribute is legal; turbo-stream `target=` looks the
/// element up via `document.getElementById`, which doesn't need CSS-style
/// escaping.
///
/// Examples:
///   * `"auto"`           → `"auto"`
///   * `"a.b"`            → `"a%2Eb"`
///   * `"a/b"`            → `"a%2Fb"`
///   * `"中"`             → `"%E4%B8%AD"`
pub fn queue_slug(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

#[derive(Debug, Serialize)]
pub struct WorkerInfo {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub hostname: String,
    pub pid: i32,
    pub created_at: String,
    pub last_heartbeat_at: String,
    pub seconds_since_heartbeat: i64,
    pub status: String,
    pub quiet: bool,
    pub revision: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(default = "default_page", deserialize_with = "deserialize_page")]
    pub page: u64,
    pub class_name: Option<String>,
    pub queue_name: Option<String>,
    pub status: Option<String>,
    /// Lower bound on `failed_executions.created_at`. Accepted as a
    /// chrono-parseable ISO 8601 / RFC 3339-ish string; handlers reject
    /// unparseable input with 400. Currently only honoured by the
    /// /failed-jobs/all/retry and /failed-jobs/all/delete endpoints.
    pub since: Option<String>,
    /// Upper bound on `failed_executions.created_at`. See `since`.
    pub until: Option<String>,
    /// SQL `LIKE` pattern matched against the stored error message
    /// (e.g. `Process+crashed%`). Currently only honoured by the
    /// /failed-jobs/all/retry and /failed-jobs/all/delete endpoints.
    pub error_like: Option<String>,
}

fn default_page() -> u64 {
    1
}

// Clamp `?page=0` (or any explicit 0) up to 1 so handlers can safely compute
// `(page - 1) * page_size` without underflowing u64.
fn deserialize_page<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let n = u64::deserialize(deserializer)?;
    Ok(n.max(1))
}

#[derive(Debug, Serialize)]
pub struct FilterOptions {
    pub class_names: Vec<String>,
    pub queue_names: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct FailedJobInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub error: String,
    pub failed_at: String,
}

#[derive(Debug, Serialize)]
pub struct InProgressJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub worker_info: String,
    pub started_at: String,
    pub runtime: String,
}

#[derive(Debug, Serialize)]
pub struct QueueJobInfo {
    pub id: i64,
    pub class_name: String,
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct ScheduledJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub created_at: String,
    pub scheduled_at: String,
    pub scheduled_in: String,
}

#[derive(Debug, Serialize)]
pub struct BlockedJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub concurrency_key: String,
    pub created_at: String,
    pub waiting_time: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FinishedJobInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub created_at: String,
    pub finished_at: String,
    pub runtime: String,
}

#[derive(Debug, Serialize)]
pub struct JobDetailsInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub status: String,
    pub created_at: String,
    pub failed_at: Option<String>,
    pub scheduled_at: Option<String>,
    pub scheduled_in: Option<String>,
    pub concurrency_key: Option<String>,
    pub waiting_time: Option<String>,
    pub expires_at: Option<String>,
    pub started_at: Option<String>,
    pub runtime: Option<String>,
    pub worker_id: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub backtrace: Option<String>,
    pub arguments: String,
    pub context: Option<String>,
    pub execution_id: Option<i64>,
    pub execution_history: Vec<ExecutionHistoryItem>,
}

#[derive(Debug, Serialize)]
pub struct ExecutionHistoryItem {
    pub attempt: i32,
    pub timestamp: String,
    pub status: String,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RecurringTaskInfo {
    pub id: i64,
    pub key: String,
    pub class_name: String,
    pub schedule: String,
    pub queue_name: String,
    pub priority: i32,
    pub description: Option<String>,
    pub last_run_at: Option<String>,
    pub next_run_at: Option<String>,
}
