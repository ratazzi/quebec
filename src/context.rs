use crate::error::{QuebecError, Result};
use croner::Cron;
use english_to_cron::str_cron_syntax;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::database_url::DatabaseUrl;

use tracing::{debug, error, trace, warn};

pub const WORKER_MEMORY_RECYCLE_EXIT_CODE: i32 = 75;

#[cfg(feature = "python")]
use pyo3::exceptions::PyException;
#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
pyo3::create_exception!(quebec, CustomError, PyException);
#[cfg(feature = "python")]
pyo3::create_exception!(
    quebec,
    AbortEnqueue,
    PyException,
    "Raise in before_enqueue to abort enqueueing (like Rails throw :abort)."
);

#[derive(Debug, Clone)]
pub struct ConcurrencyConstraint {
    pub key: String,
    pub limit: i32,
    pub duration: Option<chrono::Duration>,
    pub on_conflict: ConcurrencyConflict,
}

/// Lifecycle state of a claimed execution owned by this worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimState {
    /// Claimed; queued in the mpsc channel / Python work queue; not yet performing.
    Dispatched,
    /// Inside perform().
    InFlight,
    /// Performed, but after_executed cleanup failed — already executed, must NOT requeue.
    CleanupPending,
}

/// Concurrency conflict strategy - what to do when concurrency limit is reached
/// Matches Solid Queue's concurrency_on_conflict option
#[cfg_attr(feature = "python", pyclass(eq, eq_int, from_py_object))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConcurrencyConflict {
    /// Block the job until a slot becomes available (default behavior)
    #[default]
    Block = 0,
    /// Discard the job silently without executing
    Discard = 1,
}

#[cfg(feature = "python")]
#[pymethods]
impl ConcurrencyConflict {
    #[staticmethod]
    fn block() -> Self {
        ConcurrencyConflict::Block
    }

    #[staticmethod]
    fn discard() -> Self {
        ConcurrencyConflict::Discard
    }

    fn __repr__(&self) -> String {
        match self {
            ConcurrencyConflict::Block => "ConcurrencyConflict.Block".to_string(),
            ConcurrencyConflict::Discard => "ConcurrencyConflict.Discard".to_string(),
        }
    }
}

/// Rate limit conflict strategy — what to do when the sliding window is exhausted.
#[cfg_attr(feature = "python", pyclass(eq, eq_int, from_py_object))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RateLimitConflict {
    /// Reschedule the throttled job to a future scheduled_executions row (default).
    #[default]
    Reschedule = 0,
    /// Mark the throttled job as finished without executing (dedup-style).
    Discard = 1,
}

#[cfg(feature = "python")]
#[pymethods]
impl RateLimitConflict {
    #[staticmethod]
    fn reschedule() -> Self {
        RateLimitConflict::Reschedule
    }

    #[staticmethod]
    fn discard() -> Self {
        RateLimitConflict::Discard
    }

    fn __repr__(&self) -> String {
        match self {
            RateLimitConflict::Reschedule => "RateLimitConflict.Reschedule".to_string(),
            RateLimitConflict::Discard => "RateLimitConflict.Discard".to_string(),
        }
    }
}

/// Per-class sliding-window rate limit configuration captured at
/// `register_job_class` time. Stored in `AppContext.rate_limited_classes`.
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    pub max: i32,
    pub window: chrono::Duration,
    pub on_throttle: RateLimitConflict,
}

#[derive(Debug, Clone)]
pub struct TableConfig {
    pub jobs: String,
    pub ready_executions: String,
    pub claimed_executions: String,
    pub scheduled_executions: String,
    pub failed_executions: String,
    pub blocked_executions: String,
    pub recurring_executions: String,
    pub recurring_tasks: String,
    pub pauses: String,
    pub processes: String,
    pub semaphores: String,
}

impl Default for TableConfig {
    fn default() -> Self {
        Self {
            jobs: "solid_queue_jobs".to_string(),
            ready_executions: "solid_queue_ready_executions".to_string(),
            claimed_executions: "solid_queue_claimed_executions".to_string(),
            scheduled_executions: "solid_queue_scheduled_executions".to_string(),
            failed_executions: "solid_queue_failed_executions".to_string(),
            blocked_executions: "solid_queue_blocked_executions".to_string(),
            recurring_executions: "solid_queue_recurring_executions".to_string(),
            recurring_tasks: "solid_queue_recurring_tasks".to_string(),
            pauses: "solid_queue_pauses".to_string(),
            processes: "solid_queue_processes".to_string(),
            semaphores: "solid_queue_semaphores".to_string(),
        }
    }
}

impl TableConfig {
    pub fn with_prefix(prefix: &str) -> Self {
        // Strip trailing underscore to avoid double underscores (e.g., "hive_" -> "hive__jobs")
        let prefix = prefix.trim_end_matches('_');
        Self {
            jobs: format!("{prefix}_jobs"),
            ready_executions: format!("{prefix}_ready_executions"),
            claimed_executions: format!("{prefix}_claimed_executions"),
            scheduled_executions: format!("{prefix}_scheduled_executions"),
            failed_executions: format!("{prefix}_failed_executions"),
            blocked_executions: format!("{prefix}_blocked_executions"),
            recurring_executions: format!("{prefix}_recurring_executions"),
            recurring_tasks: format!("{prefix}_recurring_tasks"),
            pauses: format!("{prefix}_pauses"),
            processes: format!("{prefix}_processes"),
            semaphores: format!("{prefix}_semaphores"),
        }
    }
}

#[cfg(feature = "python")]
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct ConcurrencyStrategy {
    pub to: i64,
    pub duration: Duration,
    pub key: Py<PyAny>,
}

#[cfg(feature = "python")]
#[pymethods]
impl ConcurrencyStrategy {
    #[new]
    fn new(key: Py<PyAny>, to: Option<i64>, duration: Option<Duration>) -> Self {
        ConcurrencyStrategy {
            to: to.unwrap_or(1),
            duration: duration.unwrap_or(Duration::from_secs(3)),
            key,
        }
    }

    #[getter]
    pub fn to(&self) -> i64 {
        self.to
    }

    #[getter]
    pub fn duration(&self) -> Duration {
        self.duration
    }

    #[getter]
    pub fn key(&self) -> Py<PyAny> {
        self.key.clone()
    }
}

#[cfg(feature = "python")]
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct RescueStrategy {
    pub exceptions: Py<PyAny>,
    pub handler: Py<PyAny>,
}

#[cfg(feature = "python")]
#[pymethods]
impl RescueStrategy {
    #[new]
    fn new(exceptions: Py<PyAny>, handler: Py<PyAny>) -> Self {
        RescueStrategy {
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    #[getter]
    pub fn get_handler(&self) -> Py<PyAny> {
        self.handler.clone()
    }
}

#[cfg(feature = "python")]
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    pub wait: Duration,
    pub attempts: i64,
    pub exceptions: Py<PyAny>,
    pub handler: Option<Py<PyAny>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl RetryStrategy {
    #[new]
    fn new(
        exceptions: Py<PyAny>,
        wait: Option<Duration>,
        attempts: Option<i64>,
        handler: Option<Py<PyAny>>,
    ) -> Self {
        RetryStrategy {
            wait: wait.unwrap_or(Duration::from_secs(3)),
            attempts: attempts.unwrap_or(5),
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn wait(&self) -> Duration {
        self.wait
    }

    #[getter]
    pub fn attempts(&self) -> i64 {
        self.attempts
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    fn __repr__(&self) -> String {
        format!(
            "RetryStrategy(wait={:?}, attempts={}, exceptions={:?})",
            self.wait, self.attempts, self.exceptions
        )
    }
}

#[cfg(feature = "python")]
#[pyclass(from_py_object)]
#[derive(Debug, Clone)]
pub struct DiscardStrategy {
    pub exceptions: Py<PyAny>,
    pub handler: Option<Py<PyAny>>,
}

#[cfg(feature = "python")]
#[pymethods]
impl DiscardStrategy {
    #[new]
    fn new(exceptions: Py<PyAny>, handler: Option<Py<PyAny>>) -> Self {
        DiscardStrategy {
            exceptions,
            handler,
        }
    }

    #[getter]
    pub fn get_exceptions(&self) -> Py<PyAny> {
        self.exceptions.clone()
    }

    #[getter]
    pub fn get_handler(&self) -> Option<Py<PyAny>> {
        self.handler.clone()
    }
}

/// Replace characters that would break downstream consumers if they ended up
/// inside a queue name. Primary motivation is the control-plane router which
/// uses queue_name as an `:name` path segment (anything with `/` would 404),
/// but space / `?` / `#` / `%` / control chars are also URL-hostile and worth
/// neutralising. Returns the cleaned string and a flag indicating whether any
/// substitution happened so callers can warn the operator once.
pub(crate) fn sanitize_queue_name(raw: &str) -> (String, bool) {
    let cleaned: String = raw
        .chars()
        .map(|c| match c {
            '/' | '\\' | '?' | '#' | '%' => '-',
            c if c.is_whitespace() || c.is_control() => '-',
            c => c,
        })
        .collect();
    let changed = cleaned != raw;
    (cleaned, changed)
}

#[derive(Debug)]
pub struct AppContext {
    pub cwd: std::path::PathBuf,
    pub dsn: DatabaseUrl,
    pub db: Option<Arc<DatabaseConnection>>, // Use shared connection for SQLite
    pub connect_options: ConnectOptions,     // For creating new connections
    pub name: String, // Application name for NOTIFY channel (default: "quebec")
    pub use_skip_locked: bool,
    /// Enable PostgreSQL LISTEN/NOTIFY for low-latency job pickup.
    /// Disable when running through transaction-pooling proxies (RDS Proxy,
    /// PgBouncer transaction mode) that don't keep session state across queries.
    pub use_listen_notify: bool,
    /// Per-queue minimum interval between NOTIFYs emitted by this process.
    /// Producers that fall inside the window are silently dropped; the worker
    /// catches up via polling / IDLE fallback. Set to `Duration::ZERO` to
    /// disable throttling. Mitigates NOTIFY storms on Aurora-style clusters
    /// where every NOTIFY incurs cross-AZ replication cost.
    pub notify_throttle_interval: Duration,
    /// When set, every enqueue path rewrites `queue_name` to this value,
    /// ignoring whatever the class / call site / scheduler specified. Use
    /// for multi-branch development against a shared database: each branch
    /// runs with its own `QUEBEC_FORCE_OVERRIDE_QUEUE=branch_x` and only
    /// consumes that queue, so jobs enqueued by one branch are never picked
    /// up by another. Production deployments should leave this unset — the
    /// silent rewrite is intentional for the dev use case but surprising
    /// elsewhere.
    pub force_override_queue: Option<String>,
    pub process_heartbeat_interval: Duration,
    pub process_alive_threshold: Duration,
    pub shutdown_timeout: Duration,
    /// When a quiet signal (SIGUSR1/SIGTSTP) is received, also exit the process
    /// once all of this worker's in-flight and claimed jobs have drained — with
    /// no time limit, matching Sidekiq Enterprise's USR2 rolling restart. Opt-in
    /// (default false). Standalone-only: ignored under the fork supervisor, where
    /// a self-exited child would just be reforked — use a supervisor-level
    /// rolling restart there instead. Independent of `shutdown_timeout`, which
    /// still bounds the SIGTERM graceful-shutdown path.
    pub quiet_then_exit: bool,
    pub silence_polling: bool,
    pub preserve_finished_jobs: bool,
    pub clear_finished_jobs_after: Duration,
    pub cleanup_batch_size: u64,
    pub cleanup_interval: Duration,
    pub default_concurrency_control_period: Duration,
    pub dispatcher_polling_interval: Duration,
    pub dispatcher_batch_size: u64,
    pub dispatcher_concurrency_maintenance_interval: Duration,
    pub worker_polling_interval: Duration,
    pub worker_threads: u64,
    /// Optional worker RSS soft limit. When set, a worker that stays above the
    /// limit for `worker_memory_recycle_confirmations` consecutive samples
    /// enters quiet mode, drains, and exits with the planned recycle exit code.
    pub worker_max_rss_bytes: Option<u64>,
    pub worker_memory_check_interval: Duration,
    pub worker_memory_graceful_timeout: Duration,
    pub worker_memory_recycle_confirmations: u64,
    pub control_plane_sse_interval: Duration,
    pub worker_queues: Option<crate::config::QueueSelector>, // Queue configuration for worker
    pub graceful_shutdown: CancellationToken,
    pub force_quit: CancellationToken,
    /// Sidekiq-style "quiet" mode: when cancelled, the worker stops claiming
    /// new jobs but continues running so in-flight jobs can finish. Used for
    /// seamless restarts (signal old instance quiet, start new instance,
    /// later send SIGTERM once old instance has drained).
    pub quiet: CancellationToken,
    #[cfg(feature = "python")]
    pub runnables: Arc<RwLock<HashMap<String, crate::worker::Runnable>>>, // Store job class runnables
    pub concurrency_enabled: Arc<RwLock<HashSet<String>>>, // Store job classes with concurrency control enabled
    /// EXPERIMENTAL: per-queue concurrency limits enforced at worker
    /// claim time by acquiring a `queue:<name>` semaphore. Naming and
    /// semantics may change. Use to isolate misbehaving queues during
    /// remediation. Queues not present here are unlimited.
    pub experimental_queue_concurrency: HashMap<String, i32>,
    /// EXPERIMENTAL: per-class sliding-window rate limits. Empty means
    /// no class declared `rate_limit_max`; the worker claim path uses
    /// `.is_empty()` to skip the rate check path with zero overhead.
    pub rate_limited_classes: Arc<RwLock<HashMap<String, RateLimitConfig>>>,
    pub runtime_handle: Option<Handle>,
    pub table_config: TableConfig, // Dynamic table name configuration
    /// Optional notifier for idle worker threads - when set, signals main loop to poll for new jobs
    pub idle_notify: Arc<RwLock<Option<Arc<Notify>>>>,
    /// Supervisor PID recorded right after fork. When non-zero, the role loops
    /// periodically compare `getppid()` against it and trigger graceful shutdown
    /// if it no longer matches (i.e. the supervisor died and we got reparented
    /// to init/launchd). Zero means "not supervised, do not check".
    pub supervisor_pid: AtomicI32,
    /// True while a worker claim transaction is in flight (from before the quiet
    /// gate in `process_available_jobs` until the claimed jobs have been
    /// dispatched). `should_drain_exit` requires this to be false so a job that
    /// passed the quiet check just before quiet was signalled cannot be missed
    /// by the idle snapshot, which would otherwise let the process self-exit
    /// while that batch is still being published.
    pub claim_in_progress: AtomicBool,
    /// `(entry_index, within_entry)` populated by `apply_worker_config` /
    /// `apply_dispatcher_config` after fork so `set_proc_title` can show
    /// `[worker.<entry>.<within>:threads]` instead of all sibling processes
    /// sharing the same `[worker:threads]` line. `None` for standalone
    /// (non-supervised) runs, the supervisor parent itself, and the
    /// scheduler (which is always a single process).
    pub proc_slot: Option<(usize, usize)>,
    /// Authoritative ledger of claimed_execution ids owned by this worker,
    /// keyed by `claimed_executions.id` and mapped to their lifecycle
    /// `ClaimState`. As of Task 1 the only state used is `ClaimState::InFlight`,
    /// matching the semantics of the previous `in_flight_executions` set:
    /// inserted in `Worker::pick_job` (when a job leaves the dispatch channel
    /// for the Python work queue) and again at the start of `Execution::invoke`
    /// (idempotent, via `InFlightGuard`), removed by `after_executed` once the
    /// claimed row is deleted — or marked `CleanupPending` there if that cleanup
    /// fails — removed on `Execution`'s `Drop` as a backstop for a job that
    /// is picked but never performed (preserving any `CleanupPending` mark), and
    /// marked `CleanupPending` by `InFlightGuard`'s `Drop` when `invoke` unwinds
    /// on a panic (so a possibly-executed InFlight job is never requeued, even
    /// if the `Execution` is kept alive by the Python runner and thus never
    /// dropped).
    /// `release_all_claimed_executions` skips in-flight and cleanup-pending ids
    /// during graceful shutdown and the shutdown drain waits for the ledger to
    /// empty, so a running or about-to-run job is never handed back to a
    /// neighbour worker (which would run it a second time). Plain
    /// `Mutex<HashMap>`: the guarded section is a single
    /// insert/remove/contains/clone, never held across an await.
    pub claim_ledger: Arc<std::sync::Mutex<std::collections::HashMap<i64, ClaimState>>>,
    pub worker_memory_recycle_requested: AtomicBool,
    pub worker_memory_recycle_started_at: Mutex<Option<Instant>>,
    pub worker_last_rss_bytes: AtomicU64,
    /// Worker-local stop-the-world ownership for exclusive jobs
    /// (`Runnable.exclusive`). `0` means no exclusive owns the worker; any other
    /// value is the `claimed_executions.id` of the currently-owning exclusive.
    /// Set by `claim_jobs`' exclusive sieve via a plain `store`: production
    /// claim dispatch is single-loop (the dispatcher's main `select!`), so
    /// no concurrent setters can race; the gate observers use SeqCst loads
    /// and CAS, which serialise correctly against the store.
    /// Cleared by the owner's `after_executed` (success and emergency
    /// branches) and `Execution::Drop`, all via CAS (id → 0). All
    /// abandonment helpers (`release_claimed_batch`, `delete_claimed_by_id`,
    /// `fail_claimed_by_id`, `release_all_claimed_executions`) also CAS-
    /// clear, so a exclusive sieved but never executed (find_by_ids failure,
    /// runnable lookup failure, channel-full, etc.) doesn't leave the
    /// worker stuck. CAS ensures a stale Execution dropping LATE cannot
    /// clear a freshly-set ownership belonging to a newer exclusive.
    /// Process-local — does not coordinate across forked siblings or other
    /// worker processes.
    pub exclusive_owner: std::sync::atomic::AtomicI64,
    /// True when at least one registered class declares `exclusive = True`.
    /// Cached at registration so the `claim_jobs` fast-path can short-circuit
    /// the exclusive sieve with a single atomic load instead of acquiring the
    /// GIL and scanning the `runnables` registry on every claim batch. Set by
    /// `register_job_class`; inherited across fork (the registry is shared).
    pub any_exclusive_class: AtomicBool,
}

/// Marks a claimed_execution as in-flight at the start of a `perform()` call.
/// Construction inserts the id as `ClaimState::InFlight`. Normal-path removal is
/// NOT done here: the ledger entry is cleared by `after_executed`, which tracks
/// the real DB cleanup outcome (remove on success, mark `CleanupPending` on
/// failure). The only thing `Drop` does is a panic-only backstop (see below).
pub struct InFlightGuard {
    ledger: Arc<std::sync::Mutex<std::collections::HashMap<i64, ClaimState>>>,
    id: i64,
}

impl InFlightGuard {
    pub fn new(
        ledger: Arc<std::sync::Mutex<std::collections::HashMap<i64, ClaimState>>>,
        id: i64,
    ) -> Self {
        // Recover from poisoning: the ledger stays consistent even if a holder
        // panicked, and silently skipping the insert would let the shutdown
        // release treat a running job as releasable.
        ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, ClaimState::InFlight);
        Self { ledger, id }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        // Panic-only backstop. On a NORMAL drop do nothing: `after_executed`
        // owns the normal-path transition (remove on cleanup success,
        // CleanupPending on failure), and removing here would wipe a
        // CleanupPending mark since the guard drops right after
        // `after_executed` returns.
        //
        // On a PANIC unwind, however, `after_executed` never ran, so the
        // InFlight entry would otherwise leak forever and `ledger_has_active`
        // would block the quiet_then_exit drain. The guard is a stack local of
        // `Execution::invoke`, so it is reliably dropped when that frame
        // unwinds (the owning `Execution` may be kept alive on the Python side,
        // so its own Drop backstop may not fire). Mark the entry
        // `CleanupPending` ONLY if it is still InFlight: the job was inside
        // perform() and may have committed side effects, so it must NOT be
        // requeued. CleanupPending puts the id in
        // `release_all_claimed_executions`'s skip-set (a graceful shutdown that
        // survives the panic will not hand the row to a neighbour → no
        // duplicate run), counts as non-active so it does not block the drain
        // (`ledger_has_active` treats CleanupPending as drained), and the
        // residual claimed DB row is reclaimed by the orphan-sweep once this
        // process exits. This mirrors `after_executed`'s "cleanup failed →
        // CleanupPending" semantics. Note: `std::thread::panicking()` detects
        // panic unwinding, NOT async task cancellation — a dropped future is
        // not a panic, and that normal-drop case is already covered by
        // `Execution::Drop`.
        if !std::thread::panicking() {
            return;
        }
        let mut ledger = self.ledger.lock().unwrap_or_else(|e| e.into_inner());
        if matches!(ledger.get(&self.id), Some(ClaimState::InFlight)) {
            ledger.insert(self.id, ClaimState::CleanupPending);
        }
    }
}

/// Parse env var string as bool: true/1/yes → true, false/0/no → false
fn parse_bool_env(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

/// Parse env var string as Duration (supports f64 for sub-second precision)
pub(crate) fn parse_duration_f64_env(s: &str) -> Option<Duration> {
    s.parse::<f64>()
        .ok()
        .filter(|f| f.is_finite() && *f >= 0.0)
        .and_then(|f| Duration::try_from_secs_f64(f).ok())
        .or_else(|| s.parse::<u64>().ok().map(Duration::from_secs))
}

/// Build the `<basename>@<sha>` suffix appended to `set_proc_title`. Pure
/// function (path + revision in, string out) so it can be reasoned about
/// without spinning up an `AppContext`. Either component is dropped when
/// absent, leaving:
/// - "release-id@a1b2c3d" when both present
/// - "release-id"          when no git available
/// - "@a1b2c3d"            when path has no usable last component
/// - None                  when neither is available
///
/// `cwd` is canonicalized so Capistrano-style deploys where the working
/// dir is `<root>/current` (a symlink to `<root>/releases/<id>`) surface
/// the release id rather than the literal "current".
pub(crate) fn proctitle_suffix(cwd: &std::path::Path, revision: Option<&str>) -> Option<String> {
    let resolved = std::fs::canonicalize(cwd).ok();
    let basename = resolved
        .as_deref()
        .unwrap_or(cwd)
        .file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty())
        .map(String::from);
    match (basename, revision) {
        (Some(b), Some(s)) => Some(format!("{b}@{s}")),
        (Some(b), None) => Some(b),
        (None, Some(s)) => Some(format!("@{s}")),
        (None, None) => None,
    }
}

#[cfg(test)]
mod proctitle_suffix_tests {
    use super::proctitle_suffix;
    use std::path::Path;

    #[test]
    fn both_present_joins_with_at() {
        let cwd = std::env::current_dir().unwrap();
        let basename = cwd.file_name().unwrap().to_str().unwrap().to_string();
        assert_eq!(
            proctitle_suffix(&cwd, Some("a1b2c3d")),
            Some(format!("{basename}@a1b2c3d"))
        );
    }

    #[test]
    fn only_basename_when_revision_absent() {
        let cwd = std::env::current_dir().unwrap();
        let basename = cwd.file_name().unwrap().to_str().unwrap().to_string();
        assert_eq!(proctitle_suffix(&cwd, None), Some(basename));
    }

    #[test]
    fn only_revision_when_basename_absent() {
        assert_eq!(
            proctitle_suffix(Path::new("/"), Some("a1b2c3d")),
            Some("@a1b2c3d".to_string())
        );
    }

    #[test]
    fn none_when_both_missing() {
        assert_eq!(proctitle_suffix(Path::new("/"), None), None);
    }
}

/// Class-level scheduling metadata captured at `register_job_class` time.
/// Plain Rust types only — readable without the GIL.
#[derive(Debug, Clone)]
pub struct RunnableDefaults {
    pub queue_as: String,
    pub priority: i32,
}

impl AppContext {
    /// Record `id` in the claim ledger with the given state. Poison-recovering;
    /// never held across an await.
    pub fn ledger_set(&self, id: i64, state: ClaimState) {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, state);
    }

    /// Remove `id` from the claim ledger. Poison-recovering.
    pub fn ledger_remove(&self, id: i64) {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }

    /// True when the claim ledger has no tracked entries. Poison-recovering.
    pub fn ledger_is_empty(&self) -> bool {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// True when the ledger holds any `Dispatched` or `InFlight` entry, i.e. work
    /// that is still pending dispatch or actively executing. `CleanupPending`
    /// entries do NOT count as active: the job already finished executing and only
    /// the DB orphan-sweep is left to reclaim the row, so a worker carrying solely
    /// `CleanupPending` entries (or none at all) is fully drained. Poison-recovering;
    /// single locked pass, never held across an await.
    pub fn ledger_has_active(&self) -> bool {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .any(|s| matches!(s, ClaimState::Dispatched | ClaimState::InFlight))
    }

    /// Count active (Dispatched or InFlight) ledger entries — how many jobs this
    /// worker is currently handling. CleanupPending entries are excluded (those
    /// already executed). Feeds the single-process systemd STATUS busy/total line.
    pub fn ledger_active_count(&self) -> usize {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|s| matches!(s, ClaimState::Dispatched | ClaimState::InFlight))
            .count()
    }

    /// Snapshot the claim ledger so callers can inspect states without holding
    /// the lock. Poison-recovering.
    pub fn ledger_snapshot(&self) -> std::collections::HashMap<i64, ClaimState> {
        self.claim_ledger
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    #[cfg(feature = "python")]
    pub fn new(
        dsn: DatabaseUrl,
        db: Option<Arc<DatabaseConnection>>,
        connect_options: ConnectOptions,
        options: Option<HashMap<String, Py<PyAny>>>,
    ) -> Self {
        let mut ctx = Self::new_inner(dsn, db, connect_options);

        // Apply configuration from kwargs and/or env vars
        // unwrap_or_default: even without kwargs, env vars (QUEBEC_*) must still be processed
        let options = options.unwrap_or_default();
        Python::attach(|py| {
            // Helper closures that warn on type mismatch
            // Priority: kwargs > env var (QUEBEC_<KEY_UPPER>) > default (None)
            let env_var = |key: &str| -> Option<String> {
                std::env::var(format!("QUEBEC_{}", key.to_uppercase())).ok()
            };
            let get_bool = |key: &str| -> Option<bool> {
                options
                        .get(key)
                        .and_then(|v| {
                            v.extract(py)
                                .map_err(|_| {
                                    warn!(
                                        "Config '{}': expected bool, got {:?}",
                                        key,
                                        v.bind(py).get_type()
                                    );
                                })
                                .ok()
                        })
                        .or_else(|| {
                            let s = env_var(key)?;
                            parse_bool_env(&s).or_else(|| {
                                warn!(
                                    "Env 'QUEBEC_{}': expected bool (true/1/yes or false/0/no), got '{}'",
                                    key.to_uppercase(), s
                                );
                                None
                            })
                        })
            };
            let get_u64 = |key: &str| -> Option<u64> {
                options
                    .get(key)
                    .and_then(|v| {
                        v.extract(py)
                            .map_err(|_| {
                                warn!(
                                    "Config '{}': expected u64, got {:?}",
                                    key,
                                    v.bind(py).get_type()
                                );
                            })
                            .ok()
                    })
                    .or_else(|| {
                        let s = env_var(key)?;
                        s.parse().ok().or_else(|| {
                            warn!(
                                "Env 'QUEBEC_{}': failed to parse '{}' as u64",
                                key.to_uppercase(),
                                s
                            );
                            None
                        })
                    })
            };
            let get_duration = |key: &str| -> Option<Duration> {
                options
                    .get(key)
                    .and_then(|v| {
                        v.extract::<Duration>(py)
                            .or_else(|_| v.extract::<u64>(py).map(Duration::from_secs))
                            .map_err(|_| {
                                warn!(
                                    "Config '{}': expected Duration or u64, got {:?}",
                                    key,
                                    v.bind(py).get_type()
                                );
                            })
                            .ok()
                    })
                    .or_else(|| {
                        let s = env_var(key)?;
                        parse_duration_f64_env(&s).or_else(|| {
                            warn!(
                                "Env 'QUEBEC_{}': failed to parse '{}' as duration",
                                key.to_uppercase(),
                                s
                            );
                            None
                        })
                    })
            };

            if let Some(v) = get_bool("use_skip_locked") {
                ctx.use_skip_locked = v;
            }
            if let Some(v) = get_bool("use_listen_notify") {
                ctx.use_listen_notify = v;
            }
            if let Some(v) = get_duration("notify_throttle_interval") {
                ctx.notify_throttle_interval = v;
            }
            if let Some(v) = get_duration("process_heartbeat_interval") {
                ctx.process_heartbeat_interval = v;
            }
            if let Some(v) = get_duration("process_alive_threshold") {
                ctx.process_alive_threshold = v;
            }
            if let Some(v) = get_duration("shutdown_timeout") {
                ctx.shutdown_timeout = v;
            }
            if let Some(v) = get_bool("silence_polling") {
                ctx.silence_polling = v;
            }
            if let Some(v) = get_bool("quiet_then_exit") {
                ctx.quiet_then_exit = v;
            }
            if let Some(v) = get_bool("preserve_finished_jobs") {
                ctx.preserve_finished_jobs = v;
            }
            if let Some(v) = get_duration("clear_finished_jobs_after") {
                ctx.clear_finished_jobs_after = v;
            }
            if let Some(v) = get_u64("cleanup_batch_size") {
                ctx.cleanup_batch_size = v;
            }
            if let Some(v) = get_duration("cleanup_interval") {
                ctx.cleanup_interval = v;
            }
            if let Some(v) = get_duration("default_concurrency_control_period") {
                ctx.default_concurrency_control_period = v;
            }
            if let Some(v) = get_duration("dispatcher_polling_interval") {
                ctx.dispatcher_polling_interval = v;
            }
            if let Some(v) = get_u64("dispatcher_batch_size") {
                ctx.dispatcher_batch_size = v;
            }
            if let Some(v) = get_duration("dispatcher_concurrency_maintenance_interval") {
                ctx.dispatcher_concurrency_maintenance_interval = v;
            }
            if let Some(v) = get_duration("control_plane_sse_interval") {
                ctx.control_plane_sse_interval = v;
            }
            // worker_polling_interval: also accepts f64 for sub-second precision
            if let Some(val) = options.get("worker_polling_interval") {
                let result = val
                    .extract::<Duration>(py)
                    .or_else(|_| val.extract::<u64>(py).map(Duration::from_secs))
                    .or_else(|_| {
                        val.extract::<f64>(py).and_then(|f| {
                            if f.is_finite() && f >= 0.0 {
                                Ok(Duration::from_secs_f64(f))
                            } else {
                                warn!("Config 'worker_polling_interval': invalid f64 value {}", f);
                                Err(pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(
                                    "invalid",
                                ))
                            }
                        })
                    });
                match result {
                    Ok(v) => ctx.worker_polling_interval = v,
                    Err(_) => warn!(
                        "Config 'worker_polling_interval': expected Duration, u64 or f64, got {:?}",
                        val.bind(py).get_type()
                    ),
                }
            } else if let Some(s) = env_var("worker_polling_interval") {
                match parse_duration_f64_env(&s) {
                    Some(v) => ctx.worker_polling_interval = v,
                    None => warn!(
                        "Env 'QUEBEC_WORKER_POLLING_INTERVAL': failed to parse '{}' as duration",
                        s
                    ),
                }
            }
            if let Some(v) = get_u64("worker_threads") {
                ctx.worker_threads = v;
            }
            if let Some(v) = get_u64("worker_max_rss_mb") {
                ctx.worker_max_rss_bytes = if v == 0 {
                    None
                } else {
                    Some(v.saturating_mul(1024 * 1024))
                };
            }
            if let Some(v) = get_duration("worker_memory_check_interval") {
                ctx.worker_memory_check_interval = v;
            }
            if let Some(v) = get_duration("worker_memory_graceful_timeout") {
                ctx.worker_memory_graceful_timeout = v;
            }
            if let Some(v) = get_u64("worker_memory_recycle_confirmations") {
                if v == 0 {
                    warn!("worker_memory_recycle_confirmations=0 ignored; using 1");
                    ctx.worker_memory_recycle_confirmations = 1;
                } else {
                    ctx.worker_memory_recycle_confirmations = v;
                }
            }
            // EXPERIMENTAL: per-queue concurrency overrides via a Python dict
            // {queue_name: limit}. Invalid entries are skipped with a warning;
            // the whole field is opt-in so leaving it unset preserves prior
            // behaviour.
            if let Some(val) = options.get("experimental_queue_concurrency") {
                match val.extract::<HashMap<String, i32>>(py) {
                    Ok(map) => {
                        let mut accepted: HashMap<String, i32> = HashMap::new();
                        for (k, v) in map {
                            let trimmed = k.trim();
                            if trimmed.is_empty() {
                                warn!("experimental_queue_concurrency: skipping empty queue name");
                                continue;
                            }
                            if v <= 0 {
                                warn!(
                                    "experimental_queue_concurrency['{}']={} ignored \
                                     (limit must be > 0; use no entry to mean unlimited)",
                                    trimmed, v
                                );
                                continue;
                            }
                            accepted.insert(trimmed.to_string(), v);
                        }
                        ctx.experimental_queue_concurrency = accepted;
                    }
                    Err(_) => warn!(
                        "Config 'experimental_queue_concurrency': expected dict[str, int], \
                         got {:?}",
                        val.bind(py).get_type()
                    ),
                }
            }
            // force_override_queue: kwargs win over the env var the Default
            // already read. Empty / whitespace-only strings clear the override
            // so callers can explicitly opt out from Python even when
            // QUEBEC_FORCE_OVERRIDE_QUEUE is set in the environment.
            if let Some(val) = options.get("force_override_queue") {
                match val.extract::<String>(py) {
                    Ok(q) => {
                        let trimmed = q.trim();
                        ctx.force_override_queue = if trimmed.is_empty() {
                            None
                        } else {
                            let (cleaned, changed) = sanitize_queue_name(trimmed);
                            if changed {
                                warn!(
                                    "force_override_queue='{}' sanitized to '{}' \
                                     (URL-hostile characters replaced with '-')",
                                    trimmed, cleaned
                                );
                            }
                            Some(cleaned)
                        };
                    }
                    Err(_) => warn!(
                        "Config 'force_override_queue': expected String, got {:?}",
                        val.bind(py).get_type()
                    ),
                }
            }
            // table_name_prefix: only apply if non-empty
            if let Some(val) = options.get("table_name_prefix") {
                match val.extract::<String>(py) {
                    Ok(prefix) if !prefix.is_empty() => {
                        ctx.table_config = TableConfig::with_prefix(&prefix);
                    }
                    Ok(_) => {} // empty string, ignore
                    Err(_) => warn!(
                        "Config 'table_name_prefix': expected String, got {:?}",
                        val.bind(py).get_type()
                    ),
                }
            } else if let Some(prefix) = env_var("table_name_prefix").filter(|s| !s.is_empty()) {
                ctx.table_config = TableConfig::with_prefix(&prefix);
            }
        });

        crate::set_silence_polling(ctx.silence_polling);
        ctx
    }

    #[cfg(not(feature = "python"))]
    pub fn new(
        dsn: DatabaseUrl,
        db: Option<Arc<DatabaseConnection>>,
        connect_options: ConnectOptions,
    ) -> Self {
        let ctx = Self::new_inner(dsn, db, connect_options);
        crate::set_silence_polling(ctx.silence_polling);
        ctx
    }

    fn new_inner(
        dsn: DatabaseUrl,
        db: Option<Arc<DatabaseConnection>>,
        connect_options: ConnectOptions,
    ) -> Self {
        Self {
            cwd: std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from(".")),
            dsn,
            db,
            connect_options,
            name: std::env::var("QUEBEC_NAME").unwrap_or_else(|_| "quebec".to_string()),
            use_skip_locked: true,
            use_listen_notify: true,
            notify_throttle_interval: Duration::from_secs(1),
            force_override_queue: std::env::var("QUEBEC_FORCE_OVERRIDE_QUEUE")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(|raw| {
                    let (cleaned, changed) = sanitize_queue_name(&raw);
                    if changed {
                        warn!(
                            "QUEBEC_FORCE_OVERRIDE_QUEUE='{}' sanitized to '{}' \
                             (URL-hostile characters replaced with '-')",
                            raw, cleaned
                        );
                    }
                    cleaned
                }),
            process_heartbeat_interval: Duration::from_secs(60),
            process_alive_threshold: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(5),
            silence_polling: true,
            quiet_then_exit: false,
            preserve_finished_jobs: true,
            clear_finished_jobs_after: Duration::from_secs(3600 * 24 * 14), // 14 days
            cleanup_batch_size: 500,
            cleanup_interval: Duration::ZERO, // disabled by default, set to enable (e.g. 3600s)
            default_concurrency_control_period: Duration::from_secs(60), // 1 minute
            dispatcher_polling_interval: Duration::from_secs(1), // 1 seconds
            dispatcher_batch_size: 500,
            dispatcher_concurrency_maintenance_interval: Duration::from_secs(600),
            worker_polling_interval: Duration::from_millis(100),
            worker_threads: 3,
            worker_max_rss_bytes: None,
            worker_memory_check_interval: Duration::from_secs(5),
            worker_memory_graceful_timeout: Duration::from_secs(300),
            worker_memory_recycle_confirmations: 3,
            control_plane_sse_interval: Duration::from_secs(5),
            worker_queues: None, // Default to all queues
            graceful_shutdown: CancellationToken::new(),
            force_quit: CancellationToken::new(),
            quiet: CancellationToken::new(),
            #[cfg(feature = "python")]
            runnables: Arc::new(RwLock::new(HashMap::new())),
            concurrency_enabled: Arc::new(RwLock::new(HashSet::new())),
            experimental_queue_concurrency: HashMap::new(),
            rate_limited_classes: Arc::new(RwLock::new(HashMap::new())),
            runtime_handle: None,
            table_config: TableConfig::default(),
            idle_notify: Arc::new(RwLock::new(None)),
            supervisor_pid: AtomicI32::new(0),
            claim_in_progress: AtomicBool::new(false),
            proc_slot: None,
            claim_ledger: Arc::new(std::sync::Mutex::new(HashMap::new())),
            worker_memory_recycle_requested: AtomicBool::new(false),
            worker_memory_recycle_started_at: Mutex::new(None),
            worker_last_rss_bytes: AtomicU64::new(0),
            exclusive_owner: std::sync::atomic::AtomicI64::new(0),
            any_exclusive_class: AtomicBool::new(false),
        }
    }

    /// Helper for callers that abandon a claimed row (release / delete /
    /// fail) before it becomes an `Execution`. Compare-exchanges `id → 0`
    /// so only the row that actually owns the exclusive seat can clear it —
    /// preventing a stale Execution drop or an unrelated release from
    /// stomping a newer exclusive's ownership. No-op when the row does not
    /// own the exclusive (the common case).
    pub fn clear_exclusive_owner_if(&self, claimed_id: i64) {
        let _ = self.exclusive_owner.compare_exchange(
            claimed_id,
            0,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn update_worker_rss(&self, rss_bytes: u64) {
        self.worker_last_rss_bytes
            .store(rss_bytes, Ordering::Relaxed);
    }

    pub fn request_worker_memory_recycle(&self, rss_bytes: u64) -> bool {
        self.update_worker_rss(rss_bytes);
        {
            let mut started_at = self
                .worker_memory_recycle_started_at
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if started_at.is_none() {
                *started_at = Some(Instant::now());
            }
        }
        !self
            .worker_memory_recycle_requested
            .swap(true, Ordering::SeqCst)
    }

    pub fn worker_memory_recycle_elapsed(&self) -> Option<Duration> {
        self.worker_memory_recycle_started_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .map(|started_at| started_at.elapsed())
    }

    pub async fn worker_drained(&self, process_id: i64) -> bool {
        if self.claim_in_progress.load(Ordering::SeqCst) {
            return false;
        }

        // Ledger gate: CleanupPending entries don't block drain (those jobs
        // already executed and only the DB orphan-sweep owns their leftover
        // rows). Active = Dispatched or InFlight.
        if self.ledger_has_active() {
            return false;
        }

        // DB gate, memory-recycle only: the ledger covers everything
        // Worker::pick_job dispatched, but external callers (drain_one in
        // tests, future helpers) may claim rows outside that flow without
        // touching the ledger. This count keeps recycle from exiting while
        // such DB rows are still owned by us. The recycle loop bounds the
        // wait with `worker_memory_graceful_timeout` so this never blocks
        // forever; `should_drain_exit` (quiet_then_exit) deliberately does
        // NOT use this helper — there is no equivalent timeout there and
        // a stuck DB-only row would hang the standalone graceful path.

        let Ok(db) = self.get_db().await else {
            return false;
        };
        matches!(
            crate::query_builder::claimed_executions::count_by_process_id(
                db.as_ref(),
                &self.table_config,
                process_id,
            )
            .await,
            Ok(0)
        )
    }

    /// Record the current parent PID so role loops can detect supervisor death.
    /// Intended to be called from a forked child right after `reset_after_fork`
    /// (mirrors Solid Queue's `Supervised#supervised_by`). A zero PID disables
    /// the check (e.g. a standalone library user has no supervisor to watch).
    pub fn watch_parent_pid(&self) {
        #[cfg(unix)]
        {
            let ppid = unsafe { libc::getppid() };
            self.supervisor_pid.store(ppid, Ordering::Relaxed);
        }
        // Windows has no fork-based supervisor; leave supervisor_pid at 0
        // so `is_orphaned` keeps reporting false.
    }

    /// Whether we were reparented since `watch_parent_pid` was called.
    /// Returns false when `watch_parent_pid` has not been invoked (the common
    /// single-process library case).
    pub fn is_orphaned(&self) -> bool {
        let stored = self.supervisor_pid.load(Ordering::Relaxed);
        if stored == 0 {
            return false;
        }
        #[cfg(unix)]
        {
            let current = unsafe { libc::getppid() };
            return current != stored;
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    pub fn set_runtime_handle(&mut self, handle: Handle) {
        self.runtime_handle = Some(handle);
    }

    pub fn get_runtime_handle(&self) -> Option<Handle> {
        self.runtime_handle.clone()
    }

    /// Produce a new `AppContext` suitable for a freshly forked child process.
    ///
    /// - Replaces the database connection and runtime handle with the caller-provided values.
    /// - Creates fresh `CancellationToken`s (the parent's tokens may already be cancelled
    ///   or associated with dropped tokio tasks).
    /// - Clears the `idle_notify` slot (the old `Notify` is tied to the parent runtime).
    /// - Preserves all configuration values and the shared registries
    ///   (`runnables`, `concurrency_enabled`) so Python-side job class registrations
    ///   keep working without re-registering in the child.
    pub fn fork_clone(&self, db: Option<Arc<DatabaseConnection>>, runtime_handle: Handle) -> Self {
        Self {
            cwd: self.cwd.clone(),
            dsn: self.dsn.clone(),
            db,
            connect_options: self.connect_options.clone(),
            name: self.name.clone(),
            use_skip_locked: self.use_skip_locked,
            process_heartbeat_interval: self.process_heartbeat_interval,
            process_alive_threshold: self.process_alive_threshold,
            shutdown_timeout: self.shutdown_timeout,
            silence_polling: self.silence_polling,
            quiet_then_exit: self.quiet_then_exit,
            preserve_finished_jobs: self.preserve_finished_jobs,
            clear_finished_jobs_after: self.clear_finished_jobs_after,
            cleanup_batch_size: self.cleanup_batch_size,
            cleanup_interval: self.cleanup_interval,
            default_concurrency_control_period: self.default_concurrency_control_period,
            dispatcher_polling_interval: self.dispatcher_polling_interval,
            dispatcher_batch_size: self.dispatcher_batch_size,
            dispatcher_concurrency_maintenance_interval: self
                .dispatcher_concurrency_maintenance_interval,
            worker_polling_interval: self.worker_polling_interval,
            worker_threads: self.worker_threads,
            worker_max_rss_bytes: self.worker_max_rss_bytes,
            worker_memory_check_interval: self.worker_memory_check_interval,
            worker_memory_graceful_timeout: self.worker_memory_graceful_timeout,
            worker_memory_recycle_confirmations: self.worker_memory_recycle_confirmations,
            control_plane_sse_interval: self.control_plane_sse_interval,
            worker_queues: self.worker_queues.clone(),
            graceful_shutdown: CancellationToken::new(),
            force_quit: CancellationToken::new(),
            quiet: CancellationToken::new(),
            #[cfg(feature = "python")]
            runnables: self.runnables.clone(),
            concurrency_enabled: self.concurrency_enabled.clone(),
            experimental_queue_concurrency: self.experimental_queue_concurrency.clone(),
            rate_limited_classes: self.rate_limited_classes.clone(),
            runtime_handle: Some(runtime_handle),
            table_config: self.table_config.clone(),
            idle_notify: Arc::new(RwLock::new(None)),
            // Fresh state; child must call `watch_parent_pid` after fork.
            supervisor_pid: AtomicI32::new(0),
            claim_in_progress: AtomicBool::new(false),
            // Preserve so re-forks (or apply_*_config re-using a forked ctx)
            // keep the slot label until explicitly reset by apply_*_config.
            proc_slot: self.proc_slot,
            // Fresh per-process tracking: the child runs its own worker /
            // dispatch channel, so claim-ledger ids must not be shared with the
            // parent (whose claims belong to a different process record).
            claim_ledger: Arc::new(std::sync::Mutex::new(HashMap::new())),
            worker_memory_recycle_requested: AtomicBool::new(false),
            worker_memory_recycle_started_at: Mutex::new(None),
            worker_last_rss_bytes: AtomicU64::new(0),
            // Fresh per-process exclusive ownership — exclusive ownership does
            // not cross fork; the child has its own ledger lifecycle.
            exclusive_owner: std::sync::atomic::AtomicI64::new(0),
            // The runnables registry is shared across fork, so the
            // exclusive-class presence flag carries over unchanged.
            any_exclusive_class: AtomicBool::new(
                self.any_exclusive_class
                    .load(std::sync::atomic::Ordering::Relaxed),
            ),
            use_listen_notify: self.use_listen_notify,
            notify_throttle_interval: self.notify_throttle_interval,
            force_override_queue: self.force_override_queue.clone(),
        }
    }

    async fn get_db_inner(&self) -> Result<Arc<DatabaseConnection>, DbErr> {
        if let Some(db) = &self.db {
            return Ok(Arc::clone(db));
        }
        let conn = Database::connect(self.connect_options.clone()).await?;
        Ok(Arc::new(conn))
    }

    pub async fn get_db(&self) -> Result<Arc<DatabaseConnection>, DbErr> {
        const MAX_RETRIES: u32 = 3;
        let mut attempt: u32 = 0;
        loop {
            match self.get_db_inner().await {
                Ok(db) => return Ok(db),
                Err(e) => {
                    attempt += 1;
                    if attempt >= MAX_RETRIES {
                        error!(
                            "Failed to get database connection after {} retries: {}",
                            MAX_RETRIES, e
                        );
                        return Err(e);
                    }
                    warn!(
                        "Failed to get database connection, retrying ({}/{}): {}",
                        attempt, MAX_RETRIES, e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempt as u64))
                        .await;
                }
            }
        }
    }

    /// Check if the database is PostgreSQL
    pub fn is_postgres(&self) -> bool {
        self.dsn.is_postgres()
    }

    /// Get a runnable by class name
    #[cfg(feature = "python")]
    pub fn get_runnable(&self, class_name: &str) -> Result<crate::worker::Runnable> {
        let runnables = self
            .runnables
            .read()
            .map_err(|e| QuebecError::Runtime(format!("Failed to acquire read lock: {e}")))?;
        let runnable =
            runnables
                .get(class_name)
                .ok_or_else(|| QuebecError::JobClassNotRegistered {
                    class_name: class_name.to_string(),
                })?;

        // Clone with GIL since Runnable contains Py<PyAny>
        Python::attach(|py| Ok(runnable.clone_with_gil(py)))
    }

    /// Get hook flags for a job class without cloning the full Runnable.
    #[cfg(feature = "python")]
    pub fn get_hook_flags(&self, class_name: &str) -> crate::worker::HookFlags {
        self.runnables
            .read()
            .ok()
            .and_then(|r| r.get(class_name).map(|r| r.hooks.clone()))
            .unwrap_or_default()
    }

    /// Get the registered queue/priority defaults for a job class. Avoids the
    /// GIL by skipping the `Py<PyAny>` clone that `get_runnable` performs, so
    /// callers that only need scheduling metadata pay just a HashMap lookup.
    #[cfg(feature = "python")]
    pub fn get_runnable_defaults(&self, class_name: &str) -> Option<RunnableDefaults> {
        self.runnables.read().ok().and_then(|r| {
            r.get(class_name).map(|r| RunnableDefaults {
                queue_as: r.queue_as.clone(),
                priority: r.priority as i32,
            })
        })
    }

    /// No-op fallback when the `python` feature is disabled — there is no
    /// runnable registry without Python job classes.
    #[cfg(not(feature = "python"))]
    pub fn get_runnable_defaults(&self, _class_name: &str) -> Option<RunnableDefaults> {
        None
    }

    /// Get all registered runnable class names
    #[cfg(feature = "python")]
    pub fn get_runnable_names(&self) -> Vec<String> {
        self.runnables
            .read()
            .map_or_else(|_| Vec::new(), |r| r.keys().cloned().collect())
    }

    /// Check if a job class has concurrency control enabled
    pub fn has_concurrency_control(&self, class_name: &str) -> bool {
        self.concurrency_enabled
            .read()
            .map(|c| c.contains(class_name))
            .unwrap_or(false)
    }

    /// Enable concurrency control for a job class
    pub fn enable_concurrency_control(&self, class_name: String) {
        if let Ok(mut concurrency_enabled) = self.concurrency_enabled.write() {
            concurrency_enabled.insert(class_name);
        }
    }

    /// Set process title for better visibility in system tools (htop, ps, etc.)
    /// Base format: `quebec-app_name [role:details] <cwd_basename>@<git_sha>`.
    ///
    /// `role` is `process_type` by default; when the context has
    /// `proc_slot = Some((entry, within))` (set by `apply_worker_config` /
    /// `apply_dispatcher_config` in supervisor children) it becomes
    /// `process_type#<entry>/<within>` so sibling workers / dispatchers
    /// can be told apart in `ps` — `#N` names the queue.yml entry, `/M`
    /// the process index inside it.
    ///
    /// The suffix is computed by [`proctitle_suffix`] from the running
    /// process's cwd (with symlinks resolved, so a Capistrano-style
    /// `current` → `releases/<id>` deploy surfaces `<id>` instead of
    /// `current`) and the cached `git rev-parse --short HEAD` output.
    /// Either component is dropped when missing, so the suffix is omitted
    /// entirely for plain checkouts without `.git` in a path with no last
    /// component.
    /// Examples:
    /// - quebec-myapp [worker:3] myapp@a1b2c3d              (standalone local)
    /// - quebec-coms [worker:10] 20260523-1738-b5b146b@b5b146b  (Capistrano)
    /// - quebec-coms [worker#0/1:5] release@sha             (supervised, slot 1)
    /// - quebec-coms [dispatcher#1/0] release@sha           (supervised dispatcher)
    /// - quebec-myapp [dispatcher] myapp                    (no git available)
    pub fn set_proc_title(&self, process_type: &str, details: Option<&str>) {
        let role = match self.proc_slot {
            Some((entry, within)) => format!("{process_type}#{entry}/{within}"),
            None => process_type.to_string(),
        };
        let base = if let Some(details) = details {
            format!("quebec-{} [{}:{}]", self.name, role, details)
        } else {
            format!("quebec-{} [{}]", self.name, role)
        };
        let title = match proctitle_suffix(&self.cwd, crate::process::process_revision()) {
            Some(suffix) => format!("{base} {suffix}"),
            None => base,
        };

        #[cfg(target_os = "macos")]
        {
            crate::proctitle_macos::set_title(&title);
        }

        #[cfg(any(
            target_os = "linux",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd"
        ))]
        {
            crate::proctitle_unix::set_title(&title);
        }

        #[cfg(not(any(
            target_os = "macos",
            target_os = "linux",
            target_os = "freebsd",
            target_os = "openbsd",
            target_os = "netbsd"
        )))]
        {
            // Fallback to proctitle crate for other platforms
            proctitle::set_title(&title);
        }

        trace!("Set process title: {}", title);
    }

    /// Set the idle notifier for worker thread notifications
    pub fn set_idle_notify(&self, notify: Arc<Notify>) {
        if let Ok(mut idle_notify) = self.idle_notify.write() {
            *idle_notify = Some(notify);
        }
    }

    /// Notify that a worker thread has become idle (finished a job)
    /// This triggers the main loop to poll for new jobs immediately
    pub fn notify_idle(&self) {
        if let Ok(idle_notify) = self.idle_notify.read() {
            if let Some(ref notify) = *idle_notify {
                trace!("AppContext: notifying idle");
                notify.notify_one();
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEntry {
    pub class: String,
    pub schedule: String,
    pub args: Option<Vec<serde_yaml::Value>>,
    pub key: Option<String>,
    pub queue: Option<String>,
    pub priority: Option<i32>,
}

impl ScheduledEntry {
    pub fn as_cron(&self) -> Result<Cron> {
        let mut expr = self.schedule.clone();
        let _ = str_cron_syntax(&self.schedule).map(|s| {
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() >= 7 {
                // https://docs.rs/cron/latest/cron: sec min hour day_of_month month day_of_week year
                expr = parts[..6].join(" ");
                debug!("Job({:?}): {} -> {}", self.key, self.schedule, expr);
            }
        });

        let cron = Cron::new(&expr)
            .with_seconds_optional()
            .with_dom_and_dow()
            .parse();
        Ok(cron?)
    }
}
