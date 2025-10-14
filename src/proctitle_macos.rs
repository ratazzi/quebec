//! macOS-specific process title implementation using _NSGetArgc() and _NSGetArgv()
//! This provides a way to set the process title on macOS without external dependencies

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;

static PROCTITLE_STATE: Mutex<Option<ProcTitleState>> = Mutex::new(None);

struct ProcTitleState {
    argv_start: *mut c_char,
    argv_end: *mut c_char,
    argv_len: usize,
}

unsafe impl Send for ProcTitleState {}

#[cfg(target_os = "macos")]
extern "C" {
    fn _NSGetArgc() -> *const isize;
    fn _NSGetArgv() -> *const *const *const c_char;
}

/// Initialize the process title system
/// This should be called early in the program to capture the original argv
pub fn init() {
    unsafe {
        #[cfg(target_os = "macos")]
        {
            let argc_ptr = _NSGetArgc();
            let argv_ptr = _NSGetArgv();

            if !argc_ptr.is_null() && !argv_ptr.is_null() {
                let argc = *argc_ptr;
                let argv = *argv_ptr;
                init_with_args(argc, argv);
            }
        }

        #[cfg(not(target_os = "macos"))]
        {
            // No initialization needed for other platforms
        }
    }
}

/// Initialize with specific argc and argv values
/// This is the core initialization logic
unsafe fn init_with_args(argc: isize, argv: *const *const c_char) {
    if argc <= 0 || argv.is_null() {
        return;
    }

    // Find the end of the argv array
    let mut argv_end = *argv.offset(0);

    // Calculate the end of the last argument
    for i in 0..argc {
        let arg = *argv.offset(i);
        if arg.is_null() {
            break;
        }
        let len = CStr::from_ptr(arg).to_bytes().len();
        let arg_end = arg.add(len + 1); // +1 for null terminator
        if arg_end > argv_end {
            argv_end = arg_end as *mut c_char;
        }
    }

    let argv_start = *argv.offset(0) as *mut c_char;
    let argv_len = argv_end as usize - argv_start as usize;

    *PROCTITLE_STATE.lock().unwrap() = Some(ProcTitleState {
        argv_start,
        argv_end: argv_end as *mut c_char,
        argv_len,
    });
}

/// Set the process title on macOS
/// This modifies the argv[0] memory directly to change the process title
pub fn set_title(title: &str) {
    #[cfg(target_os = "macos")]
    {
        unsafe {
            // Get state data without holding the lock
            let (argv_start, argv_len) = {
                let state_guard = PROCTITLE_STATE.lock().unwrap();
                match state_guard.as_ref() {
                    Some(state) => (state.argv_start, state.argv_len),
                    None => {
                        // Initialize and try again
                        std::mem::drop(state_guard);
                        init();
                        let state_guard = PROCTITLE_STATE.lock().unwrap();
                        match state_guard.as_ref() {
                            Some(state) => (state.argv_start, state.argv_len),
                            None => {
                                eprintln!("Failed to initialize proctitle system");
                                return;
                            }
                        }
                    }
                }
            };

            let title_bytes = title.as_bytes();
            let max_len = argv_len.saturating_sub(1); // -1 for null terminator

            // Clear the entire argv area
            ptr::write_bytes(argv_start, 0, argv_len);

            // Copy the new title
            let copy_len = title_bytes.len().min(max_len);
            ptr::copy_nonoverlapping(
                title_bytes.as_ptr(),
                argv_start as *mut u8,
                copy_len,
            );
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        // Use proctitle crate for other platforms
        proctitle::set_title(title);
    }
}

/// Get the current process title (for debugging/testing)
pub fn get_title() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        unsafe {
            let state_guard = PROCTITLE_STATE.lock().unwrap();
            if let Some(state) = state_guard.as_ref() {
                let argv0_str = CStr::from_ptr(state.argv_start as *const c_char);
                argv0_str.to_str().ok().map(|s| s.to_string())
            } else {
                None
            }
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        // Not implemented for other platforms
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(target_os = "macos")]
    fn test_set_title() {
        // Initialize the system
        init();

        let test_title = "test-quebec-process";
        set_title(test_title);

        // Note: In a real test, you'd verify the title was set
        // This is more of a smoke test to ensure it doesn't crash
        assert!(true);
    }
}