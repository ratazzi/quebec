//! Which semaphore a job got its concurrency slot from.
//!
//! A semaphore counts free slots but does not know which jobs hold them, so a
//! job that gave its slot back by key counted as a holder of whatever semaphore
//! held that key at the time. A job that ran past `concurrency_duration` saw
//! its semaphore expire and get deleted, then gave its slot back to the one a
//! later job had created, letting one more job than the limit run (Solid Queue
//! #827).
//!
//! As in Solid Queue 1.7.2, a job records the id of the semaphore it got its
//! slot on, and the limit it got it with, in its serialized job data
//! (`jobs.arguments`), and gives the slot back only to that semaphore. Jobs
//! that got their slot before this was recorded keep giving it back by key.

use crate::context::TableConfig;
use crate::entities::quebec_jobs;
use crate::query_builder;
use serde_json::{json, Value};

/// Key in the job's serialized data, the same one Solid Queue uses.
pub const CONCURRENCY_LOCK_KEY: &str = "solid_queue_concurrency_lock";

/// What a job's data says about the concurrency slot it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcurrencyLock {
    /// Got its slot before semaphores were recorded: give it back by key.
    NotRecorded,
    /// Recorded as holding no slot: give nothing back.
    NoSlot,
    /// Holds a slot on semaphore `semaphore_id`, taken with `limit`.
    Held {
        semaphore_id: i64,
        limit: Option<i32>,
    },
}

impl ConcurrencyLock {
    pub fn read(arguments: Option<&str>) -> Self {
        let record = arguments
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| v.get(CONCURRENCY_LOCK_KEY).cloned());
        let Some(record) = record else {
            return Self::NotRecorded;
        };
        match record.get("semaphore_id").and_then(Value::as_i64) {
            Some(semaphore_id) => Self::Held {
                semaphore_id,
                limit: record
                    .get("limit")
                    .and_then(Value::as_i64)
                    .and_then(|l| i32::try_from(l).ok()),
            },
            None => Self::NoSlot,
        }
    }
}

/// `arguments` with the job's slot recorded on it. A legacy plain-array
/// payload is wrapped into the object form first, as `increment_executions`
/// does, so there is somewhere to put the record.
pub fn with_lock(arguments: Option<&str>, semaphore_id: i64, limit: i32) -> String {
    let mut obj = arguments
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| Value::Array(vec![]));
    if !obj.is_object() {
        let args = if obj.is_array() { obj } else { json!([]) };
        obj = json!({ "arguments": args, "executions": 0, "exception_executions": {} });
    }
    obj[CONCURRENCY_LOCK_KEY] = json!({ "semaphore_id": semaphore_id, "limit": limit });
    obj.to_string()
}

/// `arguments` without a slot record, for a new job built from an old one's
/// data (a retry): the slot belongs to the old job, and the new one records
/// its own when it acquires. Unparseable or recordless input is returned as is.
pub fn without_lock(arguments: &str) -> String {
    match serde_json::from_str::<Value>(arguments) {
        Ok(Value::Object(map)) if map.contains_key(CONCURRENCY_LOCK_KEY) => {
            let mut map = map;
            map.remove(CONCURRENCY_LOCK_KEY);
            Value::Object(map).to_string()
        }
        _ => arguments.to_string(),
    }
}

/// Record on `job` that it took its slot on `semaphore_id` with `limit`
/// (Solid Queue's `record_concurrency_lock`). Must run in the transaction
/// that acquired the slot.
pub async fn record<C>(
    db: &C,
    table_config: &TableConfig,
    job: &quebec_jobs::Model,
    semaphore_id: i64,
    limit: i32,
) -> Result<(), sea_orm::DbErr>
where
    C: sea_orm::ConnectionTrait,
{
    let arguments = with_lock(job.arguments.as_deref(), semaphore_id, limit);
    query_builder::jobs::update_arguments(db, table_config, job.id, &arguments).await?;
    Ok(())
}

/// Give back the concurrency slot `job` holds for `key`
/// (Solid Queue's `release_concurrency_lock` → `Semaphore.signal`).
///
/// `limit` and `duration` come from the job's class; a recorded limit takes
/// precedence, since it is the one the slot was taken with. Returns whether a
/// slot was given back, i.e. whether a blocked job may now take it.
pub async fn release<C>(
    db: &C,
    table_config: &TableConfig,
    job: &quebec_jobs::Model,
    key: &str,
    limit: i32,
    duration: Option<chrono::Duration>,
) -> Result<bool, sea_orm::DbErr>
where
    C: sea_orm::ConnectionTrait,
{
    match ConcurrencyLock::read(job.arguments.as_deref()) {
        ConcurrencyLock::Held {
            semaphore_id,
            limit: recorded,
        } => {
            crate::semaphore::release_semaphore_by_id(
                db,
                table_config,
                semaphore_id,
                recorded.unwrap_or(limit),
                duration,
            )
            .await
        }
        ConcurrencyLock::NoSlot => Ok(false),
        ConcurrencyLock::NotRecorded => {
            crate::semaphore::release_semaphore(db, table_config, key.to_string(), limit, duration)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_what_it_records() {
        let args = with_lock(Some(r#"{"arguments":[1],"executions":2}"#), 7, 3);
        assert_eq!(
            ConcurrencyLock::read(Some(&args)),
            ConcurrencyLock::Held {
                semaphore_id: 7,
                limit: Some(3)
            }
        );
        let v: Value = serde_json::from_str(&args).unwrap();
        assert_eq!(v["arguments"], json!([1]));
        assert_eq!(v["executions"], json!(2));
    }

    #[test]
    fn wraps_plain_array_payloads() {
        let args = with_lock(Some("[1, 2]"), 1, 1);
        let v: Value = serde_json::from_str(&args).unwrap();
        assert_eq!(v["arguments"], json!([1, 2]));
    }

    #[test]
    fn missing_record_is_legacy_and_null_id_holds_nothing() {
        assert_eq!(
            ConcurrencyLock::read(Some(r#"{"arguments":[]}"#)),
            ConcurrencyLock::NotRecorded
        );
        assert_eq!(ConcurrencyLock::read(None), ConcurrencyLock::NotRecorded);
        assert_eq!(
            ConcurrencyLock::read(Some(
                r#"{"solid_queue_concurrency_lock":{"semaphore_id":null,"limit":null}}"#
            )),
            ConcurrencyLock::NoSlot
        );
    }

    #[test]
    fn without_lock_strips_only_the_record() {
        let args = with_lock(Some(r#"{"arguments":[1]}"#), 7, 3);
        assert_eq!(
            ConcurrencyLock::read(Some(&without_lock(&args))),
            ConcurrencyLock::NotRecorded
        );
        assert_eq!(without_lock("[1]"), "[1]");
    }
}
