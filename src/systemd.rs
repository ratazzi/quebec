//! systemd `sd_notify(3)` integration: READY / STATUS / WATCHDOG / STOPPING.
//!
//! Active only when launched under a `Type=notify` unit (i.e. `NOTIFY_SOCKET`
//! is set); a complete no-op everywhere else, and on non-unix targets (the
//! `sd-notify` crate is unix-only, so it is gated out of Windows wheels and
//! replaced by the stubs at the bottom of this file).
//!
//! This module is pure protocol: callers pass a fully-formatted status line
//! built from *in-process* state (the supervisor's live child set, or a single
//! process's thread/component list) — never a database snapshot, which would
//! reflect a global, cross-node view including not-yet-pruned rows.
//!
//! The watchdog is driven by the main loop, not a background timer: each
//! `notify` from the loop also pets the watchdog, so a hung loop simply stops
//! petting and systemd restarts the process. No liveness timestamp needed.

#[cfg(unix)]
mod imp {
    use sd_notify::NotifyState;

    /// Send `READY=1` plus the initial status line. No-op without NOTIFY_SOCKET.
    pub fn ready(status: &str) {
        let _ = sd_notify::notify(&[NotifyState::Ready, NotifyState::Status(status)]);
    }

    /// Refresh the status line and pet the watchdog. Called from the main loop,
    /// so a hung loop stops petting and lets systemd restart the process.
    /// `watchdog_enabled()` honours WATCHDOG_PID/USEC and is cheap (two env
    /// reads), so we check it each call rather than caching process-globally.
    pub fn notify(status: &str) {
        if sd_notify::watchdog_enabled().is_some() {
            let _ = sd_notify::notify(&[NotifyState::Status(status), NotifyState::Watchdog]);
        } else {
            let _ = sd_notify::notify(&[NotifyState::Status(status)]);
        }
    }

    /// Notify systemd we are stopping (`STOPPING=1`).
    pub fn stopping() {
        let _ = sd_notify::notify(&[NotifyState::Stopping]);
    }
}

#[cfg(not(unix))]
mod imp {
    pub fn ready(_status: &str) {}
    pub fn notify(_status: &str) {}
    pub fn stopping() {}
}

pub use imp::{notify, ready, stopping};
