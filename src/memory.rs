//! Process and cgroup memory sampling.
//!
//! The cgroup half is read-only and deliberately independent of the
//! supervisor's cgroup *management* (`python/quebec/cgroup.py`): a process can
//! always read its own `memory.current` even where `/sys/fs/cgroup` is mounted
//! read-only, which is the normal case inside Docker and Kubernetes. So the
//! observability reach is much wider than the control reach.
//!
//! Every reader returns `None` rather than an error when a file is missing:
//! `memory.peak` only exists on newer kernels, `memory.events` keys come and
//! go, and a non-cgroup host must simply report nothing.

use std::cell::RefCell;

thread_local! {
    static SYSTEM: RefCell<sysinfo::System> = RefCell::new(sysinfo::System::new());
}

pub fn current_rss_bytes() -> Option<u64> {
    let pid = sysinfo::get_current_pid().ok()?;
    SYSTEM.with(|system| {
        let mut system = system.borrow_mut();
        system.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[pid]),
            sysinfo::ProcessRefreshKind::new().with_memory(),
        );
        system.process(pid).map(|process| process.memory())
    })
}

/// Counters from the cgroup's `memory.events`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupMemoryEvents {
    /// Processes killed by this cgroup's OOM.
    pub oom_kill: u64,
    /// Times allocation was throttled by `memory.high`.
    pub high: u64,
    /// Times usage hit `memory.max` (a hit is not necessarily a kill).
    pub max: u64,
}

/// Selected fields from the cgroup's `cpu.stat`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CgroupCpuStat {
    pub usage_usec: u64,
    /// Times the cgroup was throttled by `cpu.max`; non-zero means the CPU
    /// quota is actually biting.
    pub nr_throttled: u64,
}

#[cfg(target_os = "linux")]
mod imp {
    use super::{CgroupCpuStat, CgroupMemoryEvents};
    use std::path::PathBuf;

    const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
    const PROC_MOUNTS: &str = "/proc/mounts";

    /// Resolve this process's cgroup v2 directory, or `None` on a v1/hybrid
    /// host. Deliberately not cached: a supervised child is migrated into its
    /// leaf right after fork, so a value cached too early would be wrong.
    pub fn cgroup_v2_self_path() -> Option<PathBuf> {
        let mountpoint = cgroup2_mountpoint(&std::fs::read_to_string(PROC_MOUNTS).ok()?)?;
        let rel = self_cgroup_path(&std::fs::read_to_string(PROC_SELF_CGROUP).ok()?)?;
        let path = PathBuf::from(&mountpoint).join(rel.trim_start_matches('/'));
        if path.is_dir() {
            return Some(path);
        }
        // cgroupns=host inside a container: the path from /proc/self/cgroup is
        // a host path that does not resolve against the container's mount.
        let fallback = PathBuf::from(mountpoint);
        fallback.is_dir().then_some(fallback)
    }

    /// The `0::<path>` line, which only a unified hierarchy emits.
    pub(super) fn self_cgroup_path(content: &str) -> Option<String> {
        content.lines().find_map(|line| {
            let mut parts = line.splitn(3, ':');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("0"), Some(""), Some(path)) if !path.is_empty() => Some(path.to_string()),
                (Some("0"), Some(""), Some(_)) => Some("/".to_string()),
                _ => None,
            }
        })
    }

    pub(super) fn cgroup2_mountpoint(content: &str) -> Option<String> {
        content.lines().find_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            (fields.len() >= 3 && fields[2] == "cgroup2").then(|| fields[1].to_string())
        })
    }

    fn read_file(name: &str) -> Option<String> {
        std::fs::read_to_string(cgroup_v2_self_path()?.join(name)).ok()
    }

    /// Parse a single-value file holding a byte count. `max` (the kernel's
    /// "no limit" sentinel) yields `None`, same as a missing file: neither
    /// gives a number to display.
    pub(super) fn parse_limit(content: &str) -> Option<u64> {
        let trimmed = content.trim();
        if trimmed.is_empty() || trimmed == "max" {
            return None;
        }
        trimmed.parse().ok()
    }

    pub(super) fn parse_keyed(content: &str, key: &str) -> Option<u64> {
        content.lines().find_map(|line| {
            let mut fields = line.split_whitespace();
            match (fields.next(), fields.next()) {
                (Some(k), Some(v)) if k == key => v.parse().ok(),
                _ => None,
            }
        })
    }

    pub fn cgroup_memory_current() -> Option<u64> {
        parse_limit(&read_file("memory.current")?)
    }

    /// Historical peak. Only exists on newer kernels (memory.peak landed well
    /// after the rest of the memory interface), so absence is normal.
    pub fn cgroup_memory_peak() -> Option<u64> {
        parse_limit(&read_file("memory.peak")?)
    }

    pub fn cgroup_memory_max() -> Option<u64> {
        parse_limit(&read_file("memory.max")?)
    }

    pub fn cgroup_memory_high() -> Option<u64> {
        parse_limit(&read_file("memory.high")?)
    }

    pub fn cgroup_memory_events() -> Option<CgroupMemoryEvents> {
        let content = read_file("memory.events")?;
        Some(CgroupMemoryEvents {
            oom_kill: parse_keyed(&content, "oom_kill").unwrap_or(0),
            high: parse_keyed(&content, "high").unwrap_or(0),
            max: parse_keyed(&content, "max").unwrap_or(0),
        })
    }

    pub fn cgroup_cpu_stat() -> Option<CgroupCpuStat> {
        let content = read_file("cpu.stat")?;
        Some(CgroupCpuStat {
            usage_usec: parse_keyed(&content, "usage_usec").unwrap_or(0),
            // Absent unless the cpu controller is enabled for this cgroup.
            nr_throttled: parse_keyed(&content, "nr_throttled").unwrap_or(0),
        })
    }
}

#[cfg(not(target_os = "linux"))]
mod imp {
    use super::{CgroupCpuStat, CgroupMemoryEvents};
    use std::path::PathBuf;

    pub fn cgroup_v2_self_path() -> Option<PathBuf> {
        None
    }
    pub fn cgroup_memory_current() -> Option<u64> {
        None
    }
    pub fn cgroup_memory_peak() -> Option<u64> {
        None
    }
    pub fn cgroup_memory_max() -> Option<u64> {
        None
    }
    pub fn cgroup_memory_high() -> Option<u64> {
        None
    }
    pub fn cgroup_memory_events() -> Option<CgroupMemoryEvents> {
        None
    }
    pub fn cgroup_cpu_stat() -> Option<CgroupCpuStat> {
        None
    }
}

pub use imp::{
    cgroup_cpu_stat, cgroup_memory_current, cgroup_memory_events, cgroup_memory_high,
    cgroup_memory_max, cgroup_memory_peak, cgroup_v2_self_path,
};

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::imp::{cgroup2_mountpoint, parse_keyed, parse_limit, self_cgroup_path};

    #[test]
    fn reads_the_unified_line_only() {
        assert_eq!(
            self_cgroup_path("0::/system.slice/quebec.service\n").as_deref(),
            Some("/system.slice/quebec.service")
        );
        assert_eq!(self_cgroup_path("0::/\n").as_deref(), Some("/"));
    }

    #[test]
    fn rejects_v1_only_hierarchies() {
        let v1 = "12:memory:/user.slice\n11:cpu,cpuacct:/user.slice\n";
        assert_eq!(self_cgroup_path(v1), None);
        assert_eq!(self_cgroup_path(""), None);
    }

    #[test]
    fn finds_the_cgroup2_mount() {
        let unified = "cgroup2 /sys/fs/cgroup cgroup2 rw,nsdelegate 0 0\n";
        assert_eq!(
            cgroup2_mountpoint(unified).as_deref(),
            Some("/sys/fs/cgroup")
        );

        let hybrid = "cgroup /sys/fs/cgroup/memory cgroup rw,memory 0 0\n\
                      cgroup2 /sys/fs/cgroup/unified cgroup2 rw 0 0\n";
        assert_eq!(
            cgroup2_mountpoint(hybrid).as_deref(),
            Some("/sys/fs/cgroup/unified")
        );

        let v1_only = "cgroup /sys/fs/cgroup/memory cgroup rw,memory 0 0\n";
        assert_eq!(cgroup2_mountpoint(v1_only), None);
        assert_eq!(cgroup2_mountpoint(""), None);
    }

    #[test]
    fn limit_values_distinguish_unlimited_from_a_number() {
        assert_eq!(parse_limit("134217728\n"), Some(134217728));
        assert_eq!(parse_limit("0\n"), Some(0));
        assert_eq!(parse_limit("max\n"), None);
        assert_eq!(parse_limit("\n"), None);
        assert_eq!(parse_limit("garbage"), None);
    }

    #[test]
    fn keyed_files_pick_the_requested_row() {
        let events = "low 0\nhigh 4\nmax 3\noom 1\noom_kill 2\n";
        assert_eq!(parse_keyed(events, "oom_kill"), Some(2));
        assert_eq!(parse_keyed(events, "high"), Some(4));
        assert_eq!(parse_keyed(events, "max"), Some(3));
        assert_eq!(parse_keyed(events, "oom_group_kill"), None);

        let cpu = "usage_usec 123456\nuser_usec 100000\nnr_periods 10\nnr_throttled 2\n";
        assert_eq!(parse_keyed(cpu, "usage_usec"), Some(123456));
        assert_eq!(parse_keyed(cpu, "nr_throttled"), Some(2));
        assert_eq!(parse_keyed("", "usage_usec"), None);
    }
}
