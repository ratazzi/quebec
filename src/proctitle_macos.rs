//! macOS-specific process title implementation using _NSGetArgc() and _NSGetArgv()
//! This provides a way to set the process title on macOS without external dependencies

use std::ffi::CStr;
#[cfg(target_os = "macos")]
use std::ffi::CString;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;

static PROCTITLE_STATE: Mutex<Option<ProcTitleState>> = Mutex::new(None);

struct ProcTitleState {
    argv_start: *mut c_char,
    argv_len: usize,
    #[cfg(target_os = "macos")]
    _environ_copy: Option<EnvironCopy>,
}

unsafe impl Send for ProcTitleState {}

#[cfg(target_os = "macos")]
struct EnvironCopy {
    _strings: Vec<CString>,
    _pointers: Box<[*mut c_char]>,
}

#[cfg(target_os = "macos")]
extern "C" {
    fn _NSGetArgc() -> *const isize;
    fn _NSGetArgv() -> *mut *mut *mut c_char;
    fn _NSGetEnviron() -> *mut *mut *mut c_char;
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
#[cfg(target_os = "macos")]
unsafe fn init_with_args(argc: isize, argv: *mut *mut c_char) {
    if argc <= 0 || argv.is_null() {
        return;
    }

    let mut state_guard = PROCTITLE_STATE.lock().unwrap();
    if state_guard.is_some() {
        return;
    }

    let argv_start_ptr = *argv.offset(0);
    if argv_start_ptr.is_null() {
        return;
    }

    let mut area_end = argv_start_ptr;

    // Calculate the end of the last argument
    for i in 0..argc {
        let arg = *argv.offset(i);
        if arg.is_null() {
            break;
        }
        let len = CStr::from_ptr(arg).to_bytes().len();
        let arg_end = arg.add(len + 1); // +1 for null terminator
        if arg_end > area_end {
            area_end = arg_end;
        }
    }

    let argv_start = argv_start_ptr as *mut c_char;
    let mut argv_len = area_end as usize - argv_start as usize;

    if argv_len == 0 {
        return;
    }

    let (environ_copy, area_end) = match detach_and_copy_environ(area_end as *mut c_char) {
        Some((copy, new_end)) => (Some(copy), new_end),
        None => (None, area_end as *mut c_char),
    };

    argv_len = area_end as usize - argv_start as usize;

    *state_guard = Some(ProcTitleState {
        argv_start,
        argv_len,
        #[cfg(target_os = "macos")]
        _environ_copy: environ_copy,
    });
}

/// Set the process title on macOS
/// This modifies the argv[0] memory directly to change the process title
pub fn set_title(title: &str) {
    #[cfg(target_os = "macos")]
    {
        let mut state_guard = PROCTITLE_STATE.lock().unwrap();
        if state_guard.is_none() {
            drop(state_guard);
            init();
            state_guard = PROCTITLE_STATE.lock().unwrap();
        }

        if let Some(state) = state_guard.as_mut() {
            unsafe {
                state.write_title(title);
            }
        } else {
            eprintln!("Failed to initialize proctitle system");
        }
    }

    #[cfg(not(target_os = "macos"))]
    {
        // Use proctitle crate for other platforms
        proctitle::set_title(title);
    }
}

impl ProcTitleState {
    /// Write the supplied title into the reserved argv buffer.
    unsafe fn write_title(&mut self, title: &str) {
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
    }
}

/// Copy the environment onto the heap and detach it from the argv arena so that
/// we can safely reuse the original contiguous memory when updating argv[0].
#[cfg(target_os = "macos")]
unsafe fn detach_and_copy_environ(mut area_end: *mut c_char) -> Option<(EnvironCopy, *mut c_char)> {
    let environ_ptr_ptr = _NSGetEnviron();
    if environ_ptr_ptr.is_null() {
        return None;
    }

    let environ_ptr = *environ_ptr_ptr;
    if environ_ptr.is_null() {
        return None;
    }

    const MAX_ENV_VARS: usize = 65536;

    let mut env_count = 0usize;
    let mut env_iter = environ_ptr;

    while !(*env_iter).is_null() && env_count < MAX_ENV_VARS {
        let env_str = *env_iter;
        let len = CStr::from_ptr(env_str).to_bytes().len();

        if area_end.add(1) == env_str {
            area_end = env_str.add(len + 1);
        }

        env_count += 1;
        env_iter = env_iter.add(1);
    }

    if env_count == 0 || env_count >= MAX_ENV_VARS {
        return None;
    }

    let mut strings = Vec::with_capacity(env_count);
    for idx in 0..env_count {
        let env_str = *environ_ptr.add(idx);
        strings.push(CStr::from_ptr(env_str).to_owned());
    }

    let mut pointers: Vec<*mut c_char> =
        strings.iter().map(|s| s.as_ptr() as *mut c_char).collect();
    pointers.push(ptr::null_mut());

    let mut pointer_box = pointers.into_boxed_slice();
    *environ_ptr_ptr = pointer_box.as_mut_ptr();

    Some((
        EnvironCopy {
            _strings: strings,
            _pointers: pointer_box,
        },
        area_end,
    ))
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
