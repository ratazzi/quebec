//! Unix/Linux-specific process title implementation.
//! On Linux we reuse the original argv memory returned by /proc/self/stat while
//! keeping the environment untouched. Other Unix platforms fall back to the
//! `proctitle` crate or kernel facilities.

use std::os::raw::c_char;

#[cfg(target_os = "linux")]
use std::{
    cmp::min,
    ptr,
    sync::{Mutex, OnceLock},
};

// `OnceLock` ensures the argv probe runs at most once; the inner `Mutex`
// serializes the raw-pointer writes performed by `set_title` so concurrent
// callers from different worker threads never race on the argv buffer.
#[cfg(target_os = "linux")]
static PROCTITLE_STATE: OnceLock<Mutex<ProcTitleState>> = OnceLock::new();

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
struct ProcTitleState {
    argv_start: *mut c_char,
    argv_len: usize,
}

#[cfg(target_os = "linux")]
unsafe impl Send for ProcTitleState {}

/// Initialize the process title system. On Linux we capture the argv memory
/// range using /proc/self/stat. Other platforms keep the default behaviour.
pub fn init() {
    #[cfg(target_os = "linux")]
    {
        PROCTITLE_STATE.get_or_init(|| Mutex::new(unsafe { init_state() }));
    }
}

/// Linux-specific initialization using /proc/self/stat to get arg pointers.
/// Returns a null state when the probe fails; `write_title` treats that as a
/// no-op so callers fall back to `prctl(PR_SET_NAME)` only.
#[cfg(target_os = "linux")]
unsafe fn init_state() -> ProcTitleState {
    try_init_state().unwrap_or(ProcTitleState {
        argv_start: ptr::null_mut(),
        argv_len: 0,
    })
}

#[cfg(target_os = "linux")]
unsafe fn try_init_state() -> Option<ProcTitleState> {
    let stat_contents = std::fs::read_to_string("/proc/self/stat").ok()?;

    // Field 2 (comm) is wrapped in parentheses and may contain spaces.
    let end_paren = stat_contents.rfind(')')?;
    let fields_str = stat_contents[end_paren + 1..].trim();
    let fields: Vec<&str> = fields_str.split_whitespace().collect();

    // We need fields 48-50 (arg_start, arg_end, env_start). Since the split
    // above begins at field 3 (state), adjust the indexes accordingly.
    if fields.len() <= 47 {
        return None;
    }

    let arg_start = (*fields.get(45)?).parse::<usize>().ok()?;
    let arg_end = (*fields.get(46)?).parse::<usize>().ok()?;
    let env_start = fields
        .get(47)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(arg_end);

    if arg_start == 0 || arg_end <= arg_start {
        return None;
    }

    let mut argv_len = arg_end.saturating_sub(arg_start);
    if env_start > arg_start {
        argv_len = min(argv_len, env_start.saturating_sub(arg_start));
    }

    if argv_len == 0 || argv_len > 1024 * 1024 {
        return None;
    }

    Some(ProcTitleState {
        argv_start: arg_start as *mut c_char,
        argv_len,
    })
}

/// Set the process title on Unix/Linux systems.
pub fn set_title(title: &str) {
    #[cfg(target_os = "linux")]
    {
        let cell = PROCTITLE_STATE.get_or_init(|| Mutex::new(unsafe { init_state() }));
        // If a previous holder panicked we still own the buffer; recover and
        // continue, since cosmetic title writes shouldn't propagate poisoning.
        let guard = cell.lock().unwrap_or_else(|e| e.into_inner());
        unsafe {
            guard.write_title(title);
            set_title_via_prctl(title);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        proctitle::set_title(title);
    }
}

#[cfg(target_os = "linux")]
impl ProcTitleState {
    /// Write the supplied title into the reserved argv buffer.
    unsafe fn write_title(&self, title: &str) {
        if self.argv_start.is_null() || self.argv_len == 0 {
            return;
        }

        let title_bytes = title.as_bytes();
        let max_len = self.argv_len.saturating_sub(1);
        let copy_len = title_bytes.len().min(max_len);

        ptr::write_bytes(self.argv_start, 0, self.argv_len);

        if copy_len > 0 {
            ptr::copy_nonoverlapping(title_bytes.as_ptr(), self.argv_start as *mut u8, copy_len);
        }

        *self.argv_start.add(copy_len) = 0;
    }
}

/// Set title using prctl (Linux only, 15 char limit).
#[cfg(target_os = "linux")]
unsafe fn set_title_via_prctl(title: &str) {
    let truncated_title = if title.len() > 15 {
        &title[..15]
    } else {
        title
    };

    if let Ok(c_title) = std::ffi::CString::new(truncated_title) {
        libc::prctl(libc::PR_SET_NAME, c_title.as_ptr(), 0, 0, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_title() {
        init();

        let test_title = "test-quebec-process";
        set_title(test_title);

        // Smoke test to ensure no panic.
        assert!(true);
    }
}
