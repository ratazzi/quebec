//! Linux per-job observability.
//!
//! Minor/major page-fault deltas describe activity on the native thread that
//! executes a job; they are deliberately not converted into bytes. A process
//! RSS sampler records the process-wide RSS at the start and end of each job and
//! the sampled peak in between. That process window is attributable to one job
//! only in a supervisor worker where one execution thread (or an exclusive job)
//! prevents another job from running concurrently.
//!
//! The observations go on the `job.completed` log line and on `Metric`. An
//! optional CSV recorder (toggled by `SIGUSR2` or
//! `Quebec.start_job_metrics()`) writes one row per finished job for offline
//! aggregation. All recorder and aggregation state belongs to one Quebec
//! instance and is recreated after `fork()`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde::Serialize;
use tracing::{info, warn};

/// Page-fault counters of the calling thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThreadFaults {
    pub minflt: u64,
    pub majflt: u64,
}

impl ThreadFaults {
    /// Counters accumulated since `start`.
    pub fn since(self, start: ThreadFaults) -> ThreadFaults {
        ThreadFaults {
            minflt: self.minflt.saturating_sub(start.minflt),
            majflt: self.majflt.saturating_sub(start.majflt),
        }
    }
}

#[cfg(target_os = "linux")]
pub fn thread_faults() -> Option<ThreadFaults> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: RUSAGE_THREAD fills the struct for the calling thread only.
    let rc = unsafe { libc::getrusage(libc::RUSAGE_THREAD, usage.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    // SAFETY: getrusage returned 0, so the struct is initialised.
    let usage = unsafe { usage.assume_init() };
    Some(ThreadFaults {
        minflt: usage.ru_minflt as u64,
        majflt: usage.ru_majflt as u64,
    })
}

#[cfg(not(target_os = "linux"))]
pub fn thread_faults() -> Option<ThreadFaults> {
    None
}

/// Kernel thread id of the calling thread (what `top -H` and eBPF see).
#[cfg(target_os = "linux")]
pub fn native_tid() -> u64 {
    // SAFETY: gettid has no preconditions.
    unsafe { libc::gettid() as u64 }
}

#[cfg(not(target_os = "linux"))]
pub fn native_tid() -> u64 {
    0
}

/// One sampled process-RSS window around a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessRssWindow {
    id: u64,
}

/// Completed process-RSS observation for one job window.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProcessRssObservation {
    pub start_bytes: u64,
    pub peak_bytes: u64,
    pub end_bytes: u64,
    /// True only when no other job could run in this worker process.
    pub single_job: bool,
}

impl ProcessRssObservation {
    pub fn peak_delta_bytes(self) -> u64 {
        self.peak_bytes.saturating_sub(self.start_bytes)
    }
}

#[derive(Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct RssWindowState {
    start_bytes: u64,
    peak_bytes: u64,
    single_job: bool,
}

#[derive(Debug, Default)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct RssSamplerState {
    windows: HashMap<u64, RssWindowState>,
    shutdown: bool,
}

#[derive(Debug, Default)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct RssSamplerInner {
    state: Mutex<RssSamplerState>,
    wake: Condvar,
}

/// Lazily-started process RSS sampler shared by one Quebec instance.
#[derive(Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub struct ProcessRssSampler {
    next_id: AtomicU64,
    owner_pid: u32,
    inner: Arc<RssSamplerInner>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

#[cfg(target_os = "linux")]
const RSS_SAMPLE_INTERVAL: Duration = Duration::from_millis(100);

impl Default for ProcessRssSampler {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            owner_pid: std::process::id(),
            inner: Arc::new(RssSamplerInner::default()),
            thread: Mutex::new(None),
        }
    }
}

impl ProcessRssSampler {
    /// Begin a job window. Linux-only: callers on other targets get `None`.
    #[cfg(target_os = "linux")]
    pub fn begin(&self, single_job: bool) -> Option<ProcessRssWindow> {
        let start_bytes = crate::memory::current_rss_bytes()?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let window = ProcessRssWindow { id };
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.windows.insert(
                id,
                RssWindowState {
                    start_bytes,
                    peak_bytes: start_bytes,
                    single_job,
                },
            );
        }
        self.ensure_thread();
        self.inner.wake.notify_one();
        Some(window)
    }

    #[cfg(not(target_os = "linux"))]
    pub fn begin(&self, _single_job: bool) -> Option<ProcessRssWindow> {
        None
    }

    /// Finish a job window, forcing one final RSS sample before removing it.
    #[cfg(target_os = "linux")]
    pub fn finish(&self, window: ProcessRssWindow) -> Option<ProcessRssObservation> {
        let end_bytes = crate::memory::current_rss_bytes();
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut active = state.windows.remove(&window.id)?;
        let end_bytes = end_bytes?;
        active.peak_bytes = active.peak_bytes.max(end_bytes);
        Some(ProcessRssObservation {
            start_bytes: active.start_bytes,
            peak_bytes: active.peak_bytes,
            end_bytes,
            single_job: active.single_job,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn finish(&self, _window: ProcessRssWindow) -> Option<ProcessRssObservation> {
        None
    }

    #[cfg(target_os = "linux")]
    fn ensure_thread(&self) {
        let mut thread = self.thread.lock().unwrap_or_else(|e| e.into_inner());
        if thread.is_some() {
            return;
        }
        let inner = self.inner.clone();
        match std::thread::Builder::new()
            .name("quebec-job-rss".into())
            .spawn(move || rss_sampler_loop(inner))
        {
            Ok(handle) => *thread = Some(handle),
            Err(error) => {
                warn!(%error, "Failed to start job RSS sampler; only boundary samples will be available");
            }
        }
    }
}

impl Drop for ProcessRssSampler {
    fn drop(&mut self) {
        // A forked child cannot join a thread inherited from its parent. Its
        // fresh AppContext owns a fresh sampler; simply detach the stale handle.
        if self.owner_pid != std::process::id() {
            return;
        }
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.shutdown = true;
            state.windows.clear();
        }
        self.inner.wake.notify_all();
        if let Some(thread) = self
            .thread
            .get_mut()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = thread.join();
        }
    }
}

#[cfg(target_os = "linux")]
fn rss_sampler_loop(inner: Arc<RssSamplerInner>) {
    loop {
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.windows.is_empty() && !state.shutdown {
            state = inner.wake.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if state.shutdown {
            return;
        }
        if let Some(rss_bytes) = crate::memory::current_rss_bytes() {
            for window in state.windows.values_mut() {
                window.peak_bytes = window.peak_bytes.max(rss_bytes);
            }
        }
        drop(state);
        std::thread::sleep(RSS_SAMPLE_INTERVAL);
    }
}

/// Per-instance memory-observability state.
#[derive(Debug, Default)]
pub struct JobMetrics {
    recorder: Recorder,
    aggregator: Aggregator,
    rss_sampler: ProcessRssSampler,
}

impl JobMetrics {
    pub fn recorder(&self) -> &Recorder {
        &self.recorder
    }

    pub fn aggregator(&self) -> &Aggregator {
        &self.aggregator
    }

    pub fn rss_sampler(&self) -> &ProcessRssSampler {
        &self.rss_sampler
    }
}

/// One finished job, as written to the CSV file.
#[derive(Clone, Debug, Serialize)]
pub struct JobRecord {
    /// Unix epoch milliseconds when the job finished.
    pub ts_ms: i64,
    pub pid: u32,
    pub tid: u64,
    pub jid: String,
    pub class: String,
    pub queue: String,
    pub status: &'static str,
    pub duration_ms: f64,
    pub minor_faults: Option<u64>,
    pub major_faults: Option<u64>,
    /// Process RSS at the job boundaries and the sampled peak in between.
    pub process_rss_start_kb: Option<u64>,
    pub process_rss_peak_kb: Option<u64>,
    pub process_rss_end_kb: Option<u64>,
    pub process_rss_peak_delta_kb: Option<u64>,
    /// Whether this worker process could run only this job during the window.
    pub process_rss_single_job: bool,
    /// Jobs claimed or in flight in this process when the job finished.
    pub active_jobs: usize,
}

impl JobRecord {
    /// CSV header, in field order. Keep in sync with the struct.
    pub const COLUMNS: [&'static str; 16] = [
        "ts_ms",
        "pid",
        "tid",
        "jid",
        "class",
        "queue",
        "status",
        "duration_ms",
        "minor_faults",
        "major_faults",
        "process_rss_start_kb",
        "process_rss_peak_kb",
        "process_rss_end_kb",
        "process_rss_peak_delta_kb",
        "process_rss_single_job",
        "active_jobs",
    ];
}

/// Summary returned when a recording stops.
#[derive(Clone, Debug)]
pub struct RecordingSummary {
    pub path: PathBuf,
    pub rows: u64,
    pub dropped: u64,
}

#[derive(Debug)]
struct ActiveRecording {
    tx: Option<SyncSender<JobRecord>>,
    accepting: Arc<AtomicBool>,
    path: PathBuf,
    pid: u32,
    started: Instant,
    max_rows: u64,
    max_duration: Duration,
    sent: u64,
    dropped: Arc<AtomicU64>,
    writer: Option<JoinHandle<u64>>,
}

#[derive(Debug, Default)]
struct RecorderState {
    current: Option<ActiveRecording>,
    draining: Vec<ActiveRecording>,
}

#[derive(Debug, Default)]
struct RecorderInner {
    active: AtomicBool,
    state: Mutex<RecorderState>,
}

impl RecorderInner {
    fn take_current(&self, state: &mut RecorderState) -> Option<ActiveRecording> {
        let mut active = state.current.take()?;
        self.active.store(false, Ordering::Release);
        active.accepting.store(false, Ordering::Release);
        // Disconnect now: the writer drains accepted rows without a reaper.
        active.tx.take();
        Some(active)
    }

    fn retire_current(&self, state: &mut RecorderState, reason: &'static str) -> bool {
        if let Some(active) = self.take_current(state) {
            info!(
                path = %active.path.display(),
                reason,
                "Job metrics recording stopped"
            );
            state.draining.retain(|a| {
                a.writer
                    .as_ref()
                    .is_some_and(|writer| !writer.is_finished())
            });
            state.draining.push(active);
            true
        } else {
            false
        }
    }

    fn expire(&self, accepting: &Arc<AtomicBool>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        // An old writer must never stop a newer recording.
        if state
            .current
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(&active.accepting, accepting))
        {
            self.retire_current(&mut state, "time limit");
        }
    }
}

/// Per-Quebec-instance CSV recorder. At most one recording at a time.
#[derive(Debug)]
pub struct Recorder {
    inner: Arc<RecorderInner>,
    owner_pid: u32,
}

impl Default for Recorder {
    fn default() -> Self {
        Self {
            inner: Arc::new(RecorderInner::default()),
            owner_pid: std::process::id(),
        }
    }
}

impl Drop for Recorder {
    fn drop(&mut self) {
        // Never lock or join state inherited from vanished parent threads.
        if self.owner_pid == std::process::id() {
            self.stop();
        }
    }
}

const CHANNEL_CAPACITY: usize = 4096;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_ROWS: u64 = 100_000;
const DEFAULT_MAX_SECONDS: u64 = 3600;

fn env_u64(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(value) if value > 0 => value,
            _ => {
                warn!(key, value = %raw, default, "Invalid job metrics limit; using default");
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(raw)) => {
            warn!(key, value = ?raw, default, "Invalid job metrics limit; using default");
            default
        }
    }
}

/// Default output file: `$QUEBEC_JOB_METRICS_DIR` (or the OS temp dir) /
/// `quebec-job-metrics-<pid>-<timestamp>.csv`.
pub fn default_output_path() -> PathBuf {
    let dir = std::env::var_os("QUEBEC_JOB_METRICS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%S%.9fZ");
    dir.join(format!(
        "quebec-job-metrics-{}-{stamp}.csv",
        std::process::id()
    ))
}

impl Recorder {
    /// Cheap check for the hot path.
    pub fn is_active(&self) -> bool {
        self.inner.active.load(Ordering::Relaxed)
    }

    /// Start writing rows to `path` (or [`default_output_path`]). Returns the
    /// path in use; an error if a recording is already running or the file
    /// cannot be created.
    pub fn start(&self, path: Option<&Path>) -> std::io::Result<PathBuf> {
        self.start_with_limits(
            path,
            env_u64("QUEBEC_JOB_METRICS_MAX_ROWS", DEFAULT_MAX_ROWS),
            Duration::from_secs(env_u64(
                "QUEBEC_JOB_METRICS_MAX_SECONDS",
                DEFAULT_MAX_SECONDS,
            )),
            FLUSH_INTERVAL,
        )
    }

    fn start_with_limits(
        &self,
        path: Option<&Path>,
        max_rows: u64,
        max_duration: Duration,
        flush_interval: Duration,
    ) -> std::io::Result<PathBuf> {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.current.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "job metrics recording already active",
            ));
        }
        let path = path
            .map(Path::to_path_buf)
            .unwrap_or_else(default_output_path);
        let file = std::fs::File::create(&path)?;
        let (tx, rx) = sync_channel::<JobRecord>(CHANNEL_CAPACITY);
        let writer_path = path.clone();
        let started = Instant::now();
        let accepting = Arc::new(AtomicBool::new(true));
        let writer_accepting = accepting.clone();
        let inner = Arc::downgrade(&self.inner);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer_dropped = dropped.clone();
        let writer = std::thread::Builder::new()
            .name("quebec-job-metrics".into())
            .spawn(move || {
                let mut writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(std::io::BufWriter::new(file));
                // Written up front so an empty recording still has its header.
                if let Err(e) = writer
                    .write_record(JobRecord::COLUMNS)
                    .and_then(|()| Ok(writer.flush()?))
                {
                    warn!(path = %writer_path.display(), "job metrics header write failed: {e}");
                }
                let mut rows = 0u64;
                let mut last_flush = Instant::now();
                loop {
                    let mut wait = flush_interval.saturating_sub(last_flush.elapsed());
                    if writer_accepting.load(Ordering::Acquire) {
                        let remaining = max_duration.saturating_sub(started.elapsed());
                        if remaining.is_zero() {
                            if let Some(inner) = inner.upgrade() {
                                inner.expire(&writer_accepting);
                            }
                        } else {
                            wait = wait.min(remaining);
                        }
                    }
                    match rx.recv_timeout(wait) {
                        Ok(record) => match writer.serialize(&record) {
                            Ok(()) => rows += 1,
                            Err(e) => warn!(path = %writer_path.display(), "job metrics write failed: {e}"),
                        },
                        Err(RecvTimeoutError::Timeout) => {}
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                    if last_flush.elapsed() >= flush_interval {
                        if let Err(e) = writer.flush() {
                            warn!(path = %writer_path.display(), "job metrics flush failed: {e}");
                        }
                        last_flush = Instant::now();
                    }
                }
                if let Err(e) = writer.flush() {
                    warn!(path = %writer_path.display(), "job metrics flush failed: {e}");
                }
                info!(
                    path = %writer_path.display(), rows,
                    dropped = writer_dropped.load(Ordering::Relaxed),
                    "Job metrics recording finished"
                );
                rows
            })?;
        state.draining.retain(|a| {
            a.writer
                .as_ref()
                .is_some_and(|writer| !writer.is_finished())
        });
        state.current = Some(ActiveRecording {
            tx: Some(tx),
            accepting,
            path: path.clone(),
            pid: std::process::id(),
            started,
            max_rows,
            max_duration,
            sent: 0,
            dropped,
            writer: Some(writer),
        });
        self.inner.active.store(true, Ordering::Relaxed);
        info!(
            path = %path.display(),
            max_rows,
            max_seconds = max_duration.as_secs(),
            "Job metrics recording started"
        );
        Ok(path)
    }

    /// Stop the current recording, flush, and report what was written.
    /// Returns `None` when nothing was recording.
    pub fn stop(&self) -> Option<RecordingSummary> {
        let (active, draining) = {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let active = self.inner.take_current(&mut state);
            (active, std::mem::take(&mut state.draining))
        };
        let summary = active.map(|active| {
            let summary = Self::finish(active);
            info!(
                path = %summary.path.display(),
                rows = summary.rows,
                dropped = summary.dropped,
                reason = "stopped",
                "Job metrics recording stopped"
            );
            summary
        });
        for active in draining {
            Self::finish(active);
        }
        summary
    }

    /// Start if idle, stop if recording. Used by the `SIGUSR2` handler.
    pub fn toggle(&self) -> std::io::Result<Option<RecordingSummary>> {
        if self.is_active() {
            Ok(self.stop())
        } else {
            self.start(None).map(|_| None)
        }
    }

    /// Signal-safe-at-the-Python-layer toggle: stopping only disconnects the
    /// bounded queue and lets the writer drain in the background. Its handle is
    /// retained so normal shutdown can still wait for the final flush.
    /// Returns `true` when an active recording was stopped.
    pub fn toggle_in_background(&self) -> std::io::Result<bool> {
        if !self.is_active() {
            self.start(None)?;
            return Ok(false);
        }
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        Ok(self.inner.retire_current(&mut state, "stopped by signal"))
    }

    /// Path of the running recording, if any.
    pub fn current_path(&self) -> Option<PathBuf> {
        let state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.current.as_ref().map(|a| a.path.clone())
    }

    /// Queue one row. Never blocks the job thread: a full channel drops the
    /// row and bumps the dropped counter. Also enforces the row/time limits
    /// and forgets recordings inherited across `fork()`.
    pub fn record(&self, record: JobRecord) {
        if !self.is_active() {
            return;
        }
        {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            let Some(active) = state.current.as_mut() else {
                return;
            };
            if active.pid != std::process::id() {
                // Defensive fork guard. AppContext normally creates a fresh
                // recorder in each child before it can record a job.
                state.current = None;
                self.inner.active.store(false, Ordering::Release);
                return;
            }
            if active.started.elapsed() >= active.max_duration {
                self.inner.retire_current(&mut state, "time limit");
                return;
            }
            let Some(tx) = &active.tx else {
                return;
            };
            let reason = match tx.try_send(record) {
                Ok(()) => {
                    active.sent += 1;
                    (active.sent >= active.max_rows).then_some("limit reached")
                }
                Err(TrySendError::Full(_)) => {
                    active.dropped.fetch_add(1, Ordering::Relaxed);
                    None
                }
                Err(TrySendError::Disconnected(_)) => Some("writer thread gone"),
            };
            if let Some(reason) = reason {
                self.inner.retire_current(&mut state, reason);
            }
        }
    }

    fn finish(mut active: ActiveRecording) -> RecordingSummary {
        let dropped = active.dropped.load(Ordering::Relaxed);
        drop(active.tx);
        let rows = active
            .writer
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or(0);
        RecordingSummary {
            path: active.path,
            rows,
            dropped,
        }
    }
}

/// Number of log2 buckets in [`ClassStats::rss_peak_delta_kb_hist`]: bucket 0 holds
/// 0 KiB, bucket `i` holds `[2^(i-1), 2^i)` KiB, the last bucket everything
/// above 2^(HIST_BUCKETS-2) KiB (≈ 2 TiB).
pub const HIST_BUCKETS: usize = 32;

/// Values from one completed job used by the in-process aggregator.
pub struct JobObservation<'a> {
    pub class: &'a str,
    pub jid: &'a str,
    pub ok: bool,
    pub duration_ms: f64,
    pub minor_faults: Option<u64>,
    /// Present only when the RSS window was attributable to this job.
    pub rss_peak_delta_kb: Option<u64>,
}

/// Running aggregates for one job class, kept in-process since startup (or
/// the last reset). Cheap enough to be always on: one mutex + hash lookup per
/// finished job.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ClassStats {
    pub count: u64,
    pub failed: u64,
    pub duration_ms_sum: f64,
    pub duration_ms_max: f64,
    pub fault_samples: u64,
    pub minor_faults_sum: u64,
    pub rss_samples: u64,
    pub rss_peak_delta_kb_sum: u64,
    pub rss_peak_delta_kb_max: u64,
    /// Job id (`jid`) of the job that set `rss_peak_delta_kb_max`.
    pub rss_peak_delta_kb_max_jid: String,
    pub rss_peak_delta_kb_hist: [u64; HIST_BUCKETS],
}

fn hist_bucket(kb: u64) -> usize {
    if kb == 0 {
        0
    } else {
        ((u64::BITS - kb.leading_zeros()) as usize).min(HIST_BUCKETS - 1)
    }
}

/// Upper bound (KiB) of a histogram bucket.
fn hist_upper_kb(bucket: usize) -> u64 {
    if bucket == 0 {
        0
    } else {
        1u64 << bucket
    }
}

impl ClassStats {
    fn observe(&mut self, observation: &JobObservation<'_>) {
        self.count += 1;
        if !observation.ok {
            self.failed += 1;
        }
        self.duration_ms_sum += observation.duration_ms;
        self.duration_ms_max = self.duration_ms_max.max(observation.duration_ms);
        if let Some(faults) = observation.minor_faults {
            self.fault_samples += 1;
            self.minor_faults_sum += faults;
        }
        if let Some(kb) = observation.rss_peak_delta_kb {
            self.rss_samples += 1;
            self.rss_peak_delta_kb_sum += kb;
            self.rss_peak_delta_kb_hist[hist_bucket(kb)] += 1;
            if kb > self.rss_peak_delta_kb_max || self.rss_peak_delta_kb_max_jid.is_empty() {
                self.rss_peak_delta_kb_max = kb;
                self.rss_peak_delta_kb_max_jid = observation.jid.to_string();
            }
        }
    }

    pub fn duration_ms_avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.duration_ms_sum / self.count as f64
        }
    }

    pub fn rss_peak_delta_kb_avg(&self) -> Option<u64> {
        self.rss_peak_delta_kb_sum.checked_div(self.rss_samples)
    }

    pub fn rss_peak_delta_kb_max(&self) -> Option<u64> {
        (self.rss_samples > 0).then_some(self.rss_peak_delta_kb_max)
    }

    pub fn rss_peak_delta_kb_max_jid(&self) -> Option<&str> {
        (self.rss_samples > 0).then_some(self.rss_peak_delta_kb_max_jid.as_str())
    }

    /// Approximate percentile of single-job RSS peak deltas from the log2 histogram: the
    /// upper bound of the bucket containing the `q`-th sample (0.0..=1.0).
    pub fn rss_peak_delta_kb_percentile(&self, q: f64) -> Option<u64> {
        let total: u64 = self.rss_peak_delta_kb_hist.iter().sum();
        if total == 0 {
            return None;
        }
        let target = ((total as f64) * q.clamp(0.0, 1.0)).ceil().max(1.0) as u64;
        let mut seen = 0u64;
        for (bucket, n) in self.rss_peak_delta_kb_hist.iter().enumerate() {
            seen += n;
            if seen >= target {
                return Some(hist_upper_kb(bucket));
            }
        }
        Some(hist_upper_kb(HIST_BUCKETS - 1))
    }
}

/// Per-Quebec-instance per-class aggregates.
#[derive(Debug, Default)]
pub struct Aggregator {
    classes: Mutex<HashMap<String, ClassStats>>,
}

impl Aggregator {
    pub fn observe(&self, observation: &JobObservation<'_>) {
        let mut classes = self.classes.lock().unwrap_or_else(|e| e.into_inner());
        classes
            .entry(observation.class.to_string())
            .or_default()
            .observe(observation);
    }

    /// Snapshot all class stats, optionally taking and clearing them atomically.
    pub fn snapshot(&self, reset: bool) -> Vec<(String, ClassStats)> {
        let mut classes = self.classes.lock().unwrap_or_else(|e| e.into_inner());
        let snapshot = if reset {
            std::mem::take(&mut *classes)
        } else {
            classes.clone()
        };
        drop(classes);
        let mut out: Vec<_> = snapshot.into_iter().collect();
        out.sort_by(|a, b| {
            b.1.rss_peak_delta_kb_max
                .cmp(&a.1.rss_peak_delta_kb_max)
                .then_with(|| a.0.cmp(&b.0))
        });
        out
    }

    /// One `job_metrics.summary` log line per class.
    pub fn log_summary(&self) {
        let snapshot = self.snapshot(false);
        if snapshot.is_empty() {
            info!(
                event = "job_metrics.summary",
                "Job metrics summary: no jobs finished yet"
            );
            return;
        }
        for (class, s) in snapshot {
            info!(
                event = "job_metrics.summary",
                class = %class,
                count = s.count,
                failed = s.failed,
                duration_avg_ms = s.duration_ms_avg(),
                duration_max_ms = s.duration_ms_max,
                fault_samples = s.fault_samples,
                minor_faults_sum = s.minor_faults_sum,
                rss_samples = s.rss_samples,
                rss_peak_delta_avg_kb = s.rss_peak_delta_kb_avg(),
                rss_peak_delta_p50_kb = s.rss_peak_delta_kb_percentile(0.5),
                rss_peak_delta_p95_kb = s.rss_peak_delta_kb_percentile(0.95),
                rss_peak_delta_max_kb = s.rss_peak_delta_kb_max(),
                rss_peak_delta_max_jid = s.rss_peak_delta_kb_max_jid(),
                "Job metrics summary for `{class}'"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Aggregator, JobObservation, JobRecord, Recorder};
    use std::time::{Duration, Instant};

    fn record() -> JobRecord {
        JobRecord {
            ts_ms: 1,
            pid: std::process::id(),
            tid: 1,
            jid: "jid-1".into(),
            class: "Job".into(),
            queue: "default".into(),
            status: "executed",
            duration_ms: 1.0,
            minor_faults: None,
            major_faults: None,
            process_rss_start_kb: None,
            process_rss_peak_kb: None,
            process_rss_end_kb: None,
            process_rss_peak_delta_kb: None,
            process_rss_single_job: false,
            active_jobs: 1,
        }
    }

    fn rows(path: &std::path::Path) -> usize {
        csv::Reader::from_path(path).unwrap().records().count()
    }

    #[test]
    fn continuous_records_are_flushed_before_stopping() {
        let recorder = Recorder::default();
        let path = recorder
            .start_with_limits(
                None,
                1000,
                Duration::from_secs(60),
                Duration::from_millis(300),
            )
            .unwrap();
        let started = Instant::now();
        while started.elapsed() < Duration::from_millis(750) {
            recorder.record(record());
            if rows(&path) > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let visible = rows(&path);
        recorder.stop();
        std::fs::remove_file(path).unwrap();
        assert!(
            visible > 0,
            "continuous traffic must not postpone the flush"
        );
    }

    #[test]
    fn dropping_recorder_drains_pending_rows() {
        let recorder = Recorder::default();
        let path = recorder.start(None).unwrap();
        for _ in 0..100 {
            recorder.record(record());
        }
        drop(recorder);
        let visible = rows(&path);
        std::fs::remove_file(path).unwrap();
        assert_eq!(visible, 100);
    }

    #[test]
    fn idle_recording_expires_and_can_restart() {
        let recorder = Recorder::default();
        let path = recorder
            .start_with_limits(None, 100, Duration::from_millis(50), Duration::from_secs(5))
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while recorder.is_active() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(!recorder.is_active());
        assert_eq!(recorder.current_path(), None);
        recorder.record(record());
        assert!(recorder.stop().is_none());
        assert_eq!(rows(&path), 0);
        let next = recorder.start(None).unwrap();
        recorder.record(record());
        assert_eq!(recorder.stop().unwrap().rows, 1);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(next).unwrap();
    }

    #[test]
    fn close_waits_for_background_stop_and_row_limit() {
        for signal_stop in [false, true] {
            let recorder = Recorder::default();
            let path = recorder
                .start_with_limits(
                    None,
                    if signal_stop { 1000 } else { 100 },
                    Duration::from_secs(60),
                    Duration::from_secs(5),
                )
                .unwrap();
            for _ in 0..100 {
                recorder.record(record());
            }
            if signal_stop {
                assert!(recorder.toggle_in_background().unwrap());
            }
            assert!(!recorder.is_active());
            assert!(recorder.stop().is_none());
            assert_eq!(rows(&path), 100);
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn stale_deadline_cannot_stop_a_new_recording() {
        let recorder = Recorder::default();
        let path = recorder.start(None).unwrap();
        let old_session = recorder
            .inner
            .state
            .lock()
            .unwrap()
            .current
            .as_ref()
            .unwrap()
            .accepting
            .clone();
        let barrier = std::sync::Barrier::new(3);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                recorder.inner.expire(&old_session);
            });
            scope.spawn(|| {
                barrier.wait();
                recorder.stop();
            });
            barrier.wait();
        });
        let next = recorder.start(None).unwrap();
        recorder.inner.expire(&old_session);
        assert_eq!(recorder.current_path(), Some(next.clone()));
        recorder.record(record());
        assert_eq!(recorder.stop().unwrap().rows, 1);
        std::fs::remove_file(path).unwrap();
        std::fs::remove_file(next).unwrap();
    }

    #[test]
    fn missing_rss_samples_stay_unavailable() {
        let aggregator = Aggregator::default();
        aggregator.observe(&JobObservation {
            class: "SharedJob",
            jid: "jid-1",
            ok: true,
            duration_ms: 1.0,
            minor_faults: None,
            rss_peak_delta_kb: None,
        });

        let (_, stats) = aggregator.snapshot(false).pop().expect("one class");
        assert_eq!(stats.fault_samples, 0);
        assert_eq!(stats.rss_samples, 0);
        assert_eq!(stats.rss_peak_delta_kb_avg(), None);
        assert_eq!(stats.rss_peak_delta_kb_percentile(0.95), None);
        assert_eq!(stats.rss_peak_delta_kb_max(), None);
        assert_eq!(stats.rss_peak_delta_kb_max_jid(), None);
    }

    #[test]
    fn reset_takes_the_snapshot_atomically() {
        let aggregator = Aggregator::default();
        aggregator.observe(&JobObservation {
            class: "IsolatedJob",
            jid: "jid-1",
            ok: true,
            duration_ms: 2.0,
            minor_faults: Some(3),
            rss_peak_delta_kb: Some(4096),
        });

        let snapshot = aggregator.snapshot(true);
        assert_eq!(snapshot.len(), 1);
        assert!(aggregator.snapshot(false).is_empty());
    }
}
