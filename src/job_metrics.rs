//! Per-job memory metrics.
//!
//! Memory attribution inside a multi-threaded worker cannot come from process
//! RSS (every thread shares it). The one counter the kernel keeps per thread is
//! the page-fault count, so each job records the delta of
//! `getrusage(RUSAGE_THREAD).ru_minflt` across `perform()`. Multiplied by the
//! machine's fault granularity (calibrated once at first use, since fault-around
//! and mTHP map more than one 4 KiB page per fault) it yields the amount of
//! *new resident memory the job caused*. It is not a working-set figure: memory
//! reused from glibc's arenas or pymalloc pools is not counted again.
//!
//! The counters go on the `job.completed` log line and on `Metric`. An optional
//! CSV recorder (toggled by `SIGUSR2` or `Quebec.start_job_metrics()`) writes one
//! row per finished job to a file for offline aggregation. Fault counters are
//! Linux-only; the recorder itself works everywhere and leaves those columns
//! empty elsewhere.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};
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

/// Bytes of resident memory populated by a single minor fault on this machine.
///
/// 4 KiB on a plain kernel; 16 KiB or more where fault-around or mTHP is on.
/// Calibrated once by touching a 16 MiB anonymous mapping and counting faults.
pub fn fault_granularity() -> Option<u64> {
    static GRANULARITY: OnceLock<Option<u64>> = OnceLock::new();
    *GRANULARITY.get_or_init(calibrate_fault_granularity)
}

#[cfg(target_os = "linux")]
fn calibrate_fault_granularity() -> Option<u64> {
    const SIZE: usize = 16 << 20;
    // SAFETY: sysconf has no preconditions.
    let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).ok()?;
    if page == 0 {
        return None;
    }
    let before = thread_faults()?;
    // SAFETY: anonymous private mapping; checked against MAP_FAILED below.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return None;
    }
    for offset in (0..SIZE).step_by(page) {
        // SAFETY: offset < SIZE, inside the mapping just created.
        unsafe { std::ptr::write_volatile(base.cast::<u8>().add(offset), 1) };
    }
    let after = thread_faults();
    // SAFETY: base/SIZE are exactly what mmap returned.
    unsafe { libc::munmap(base, SIZE) };
    let faults = after?.since(before).minflt;
    if faults == 0 {
        return None;
    }
    let bytes_per_fault = SIZE as u64 / faults;
    let page = page as u64;
    Some((bytes_per_fault + page / 2) / page * page)
}

#[cfg(not(target_os = "linux"))]
fn calibrate_fault_granularity() -> Option<u64> {
    None
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
    pub minflt: Option<u64>,
    pub majflt: Option<u64>,
    /// `minflt` × fault granularity: new resident memory caused by the job.
    pub new_rss_kb: Option<u64>,
    /// Process RSS when the job finished (shared by all threads).
    pub proc_rss_kb: Option<u64>,
    /// Jobs claimed or in flight in this process when the job finished.
    pub active_jobs: usize,
}

impl JobRecord {
    /// CSV header, in field order. Keep in sync with the struct.
    pub const COLUMNS: [&'static str; 13] = [
        "ts_ms",
        "pid",
        "tid",
        "jid",
        "class",
        "queue",
        "status",
        "duration_ms",
        "minflt",
        "majflt",
        "new_rss_kb",
        "proc_rss_kb",
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

struct ActiveRecording {
    tx: SyncSender<JobRecord>,
    path: PathBuf,
    pid: u32,
    started: Instant,
    max_rows: u64,
    max_duration: Duration,
    sent: u64,
    dropped: Arc<AtomicU64>,
    writer: Option<JoinHandle<u64>>,
}

/// Process-wide CSV recorder. At most one recording at a time.
#[derive(Default)]
pub struct Recorder {
    active: AtomicBool,
    state: Mutex<Option<ActiveRecording>>,
}

const CHANNEL_CAPACITY: usize = 4096;
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_MAX_ROWS: u64 = 100_000;
const DEFAULT_MAX_SECONDS: u64 = 3600;

pub fn recorder() -> &'static Recorder {
    static RECORDER: OnceLock<Recorder> = OnceLock::new();
    RECORDER.get_or_init(Recorder::default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

/// Default output file: `$QUEBEC_JOB_METRICS_DIR` (or the OS temp dir) /
/// `quebec-job-metrics-<pid>-<timestamp>.csv`.
pub fn default_output_path() -> PathBuf {
    let dir = std::env::var_os("QUEBEC_JOB_METRICS_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    dir.join(format!(
        "quebec-job-metrics-{}-{stamp}.csv",
        std::process::id()
    ))
}

impl Recorder {
    /// Cheap check for the hot path.
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Start writing rows to `path` (or [`default_output_path`]). Returns the
    /// path in use; an error if a recording is already running or the file
    /// cannot be created.
    pub fn start(&self, path: Option<&Path>) -> std::io::Result<PathBuf> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.is_some() {
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
        let writer = std::thread::Builder::new()
            .name("quebec-job-metrics".into())
            .spawn(move || {
                let mut writer = csv::WriterBuilder::new()
                    .has_headers(false)
                    .from_writer(std::io::BufWriter::new(file));
                // Written up front so an empty recording still has its header.
                if let Err(e) = writer.write_record(JobRecord::COLUMNS).and_then(|()| Ok(writer.flush()?)) {
                    warn!(path = %writer_path.display(), "job metrics header write failed: {e}");
                }
                let mut rows = 0u64;
                loop {
                    match rx.recv_timeout(FLUSH_INTERVAL) {
                        Ok(record) => match writer.serialize(&record) {
                            Ok(()) => rows += 1,
                            Err(e) => warn!(path = %writer_path.display(), "job metrics write failed: {e}"),
                        },
                        Err(RecvTimeoutError::Timeout) => {
                            if let Err(e) = writer.flush() {
                                warn!(path = %writer_path.display(), "job metrics flush failed: {e}");
                            }
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    }
                }
                if let Err(e) = writer.flush() {
                    warn!(path = %writer_path.display(), "job metrics flush failed: {e}");
                }
                rows
            })?;
        let max_rows = env_u64("QUEBEC_JOB_METRICS_MAX_ROWS", DEFAULT_MAX_ROWS);
        let max_duration = Duration::from_secs(env_u64(
            "QUEBEC_JOB_METRICS_MAX_SECONDS",
            DEFAULT_MAX_SECONDS,
        ));
        *state = Some(ActiveRecording {
            tx,
            path: path.clone(),
            pid: std::process::id(),
            started: Instant::now(),
            max_rows,
            max_duration,
            sent: 0,
            dropped: Arc::new(AtomicU64::new(0)),
            writer: Some(writer),
        });
        self.active.store(true, Ordering::Relaxed);
        info!(
            path = %path.display(),
            max_rows,
            max_seconds = max_duration.as_secs(),
            fault_granularity = fault_granularity(),
            "Job metrics recording started"
        );
        Ok(path)
    }

    /// Stop the current recording, flush, and report what was written.
    /// Returns `None` when nothing was recording.
    pub fn stop(&self) -> Option<RecordingSummary> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.finish(&mut state, "stopped")
    }

    /// Start if idle, stop if recording. Used by the `SIGUSR2` handler.
    pub fn toggle(&self) -> std::io::Result<Option<RecordingSummary>> {
        if self.is_active() {
            Ok(self.stop())
        } else {
            self.start(None).map(|_| None)
        }
    }

    /// Path of the running recording, if any.
    pub fn current_path(&self) -> Option<PathBuf> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.as_ref().map(|a| a.path.clone())
    }

    /// Queue one row. Never blocks the job thread: a full channel drops the
    /// row and bumps the dropped counter. Also enforces the row/time limits
    /// and forgets recordings inherited across `fork()`.
    pub fn record(&self, record: JobRecord) {
        if !self.is_active() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let Some(active) = state.as_mut() else {
            return;
        };
        if active.pid != std::process::id() {
            // Forked child: the writer thread lives in the parent only.
            *state = None;
            self.active.store(false, Ordering::Relaxed);
            return;
        }
        match active.tx.try_send(record) {
            Ok(()) => active.sent += 1,
            Err(TrySendError::Full(_)) => {
                active.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => {
                self.finish(&mut state, "writer thread gone");
                return;
            }
        }
        if active.sent >= active.max_rows || active.started.elapsed() >= active.max_duration {
            self.finish(&mut state, "limit reached");
        }
    }

    fn finish(
        &self,
        state: &mut Option<ActiveRecording>,
        reason: &str,
    ) -> Option<RecordingSummary> {
        let mut active = state.take()?;
        self.active.store(false, Ordering::Relaxed);
        let dropped = active.dropped.load(Ordering::Relaxed);
        drop(active.tx);
        let rows = active
            .writer
            .take()
            .and_then(|h| h.join().ok())
            .unwrap_or(0);
        info!(
            path = %active.path.display(),
            rows,
            dropped,
            reason,
            "Job metrics recording stopped"
        );
        Some(RecordingSummary {
            path: active.path,
            rows,
            dropped,
        })
    }
}
