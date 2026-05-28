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
