use crate::context::{ConcurrencyConstraint, TableConfig};
use chrono::NaiveDateTime;
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

pub async fn acquire_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    concurrency_limit: i32,
    duration: Option<chrono::Duration>,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration.unwrap_or_else(|| chrono::Duration::minutes(2)); // Default to 2 minutes

    // First, try to create a new semaphore (attempt_creation)
    let create_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            format!(
                "INSERT INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (key) DO NOTHING",
                table_config.semaphores
            )
        }
        DatabaseBackend::Sqlite => {
            format!(
                "INSERT OR IGNORE INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "INSERT IGNORE INTO {} (key, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
                table_config.semaphores
            )
        }
    };

    let create_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            create_sql,
            vec![
                key.clone().into(),
                (concurrency_limit - 1).into(),
                expires_at.into(),
                now.into(),
                now.into(),
            ],
        ))
        .await?;

    if create_result.rows_affected() > 0 {
        // Successfully created semaphore, we got it!
        return Ok(true);
    }

    // Semaphore already exists, try to decrement if value > 0. The `WHERE
    // value > 0` predicate naturally covers limit == 1 too: a freed slot has
    // value = limit (= 1), which satisfies > 0, so decrement-to-0 atomically
    // re-acquires. The old `if concurrency_limit == 1 { return false; }`
    // short-circuit here mistook a row's existence for "held" and forced
    // callers to wait for `expires_at` to elapse before the dispatcher's
    // delete_expired sweep cleared the row — turning concurrency_duration
    // into a minimum cooldown rather than a crash-safety TTL.
    let decrement_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1 AND value > 0",
                table_config.semaphores
            )
        }
        DatabaseBackend::Sqlite => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value > 0",
                table_config.semaphores
            )
        }
        DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value > 0",
                table_config.semaphores
            )
        }
    };

    // Note: Parameter order differs between Postgres ($1,$2,$3) and SQLite/MySQL (? ? ?)
    let decrement_values = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now
            vec![key.into(), expires_at.into(), now.into()]
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            // SQLite/MySQL: ?=expires_at, ?=updated_at, ?=key (order in SQL)
            vec![expires_at.into(), now.into(), key.into()]
        }
    };

    let decrement_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            decrement_sql,
            decrement_values,
        ))
        .await?;

    Ok(decrement_result.rows_affected() > 0)
}

/// Convenience function to acquire semaphore using ConcurrencyConstraint
pub async fn acquire_semaphore_with_constraint<C>(
    db: &C,
    table_config: &TableConfig,
    constraint: &ConcurrencyConstraint,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    acquire_semaphore(
        db,
        table_config,
        constraint.key.clone(),
        constraint.limit,
        constraint.duration,
    )
    .await
}

/// Release a semaphore, incrementing its value up to the limit.
/// When value reaches limit (all slots free), keeps the record and updates expires_at.
/// This matches Solid Queue's behavior in Semaphore#signal -> attempt_increment.
///
/// Semaphore value model (matching Solid Queue):
/// - value = limit: all slots are free
/// - value = limit - 1: initial state after first job acquires
/// - value = 0: no slots available (all slots occupied)
/// - Acquire: decrement value (if value > 0)
/// - Release: increment value (if value < limit), or just update expires_at
pub async fn release_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    limit: i32,
    duration: Option<chrono::Duration>,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration.unwrap_or_else(|| chrono::Duration::minutes(2));

    // Try to increment the semaphore value if below max (limit)
    // value ranges from 0 to limit, where limit means all slots are free
    // This matches Solid Queue's: Semaphore.where(key: key, value: ...limit).update_all(...)
    let increment_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1 AND value < $4",
                table_config.semaphores
            )
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ? AND value < ?",
                table_config.semaphores
            )
        }
    };

    // Note: Parameter order differs between Postgres ($1,$2,$3,$4) and SQLite/MySQL (? ? ? ?)
    // Postgres uses positional params, SQLite/MySQL uses sequential order of appearance
    let increment_values = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now, $4=limit
            vec![
                key.clone().into(),
                expires_at.into(),
                now.into(),
                limit.into(),
            ]
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            // SQLite/MySQL: ?=expires_at, ?=updated_at, ?=key, ?=limit (order in SQL)
            vec![
                expires_at.into(),
                now.into(),
                key.clone().into(),
                limit.into(),
            ]
        }
    };

    let increment_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            increment_sql,
            increment_values,
        ))
        .await?;

    if increment_result.rows_affected() > 0 {
        // Successfully incremented
        return Ok(true);
    }

    // If increment didn't work, either:
    // 1. The semaphore doesn't exist
    // 2. The value is already at limit - 1 (all slots free)
    // In case 2, just update expires_at to keep the semaphore alive (don't delete!)
    let update_expires_sql = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            format!(
                "UPDATE {} SET \
             expires_at = $2, \
             updated_at = $3 \
             WHERE key = $1",
                table_config.semaphores
            )
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            format!(
                "UPDATE {} SET \
             expires_at = ?, \
             updated_at = ? \
             WHERE key = ?",
                table_config.semaphores
            )
        }
    };

    // Same parameter order issue as above
    let update_values = match db.get_database_backend() {
        DatabaseBackend::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now
            vec![key.into(), expires_at.into(), now.into()]
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            // SQLite/MySQL: ?=expires_at, ?=updated_at, ?=key (order in SQL)
            vec![expires_at.into(), now.into(), key.into()]
        }
    };

    let update_result = db
        .execute(Statement::from_sql_and_values(
            db.get_database_backend(),
            update_expires_sql,
            update_values,
        ))
        .await?;

    Ok(update_result.rows_affected() > 0)
}

// ──────────────────────────────────────────────────────────────────────
// Sliding-window rate limiting
// ──────────────────────────────────────────────────────────────────────

/// Outcome of `try_consume_rate_token`.
#[derive(Debug, Clone)]
pub enum ConsumeResult {
    Granted,
    Throttled { retry_at: NaiveDateTime },
}

/// Try to consume one rate-limit token for `(class_name, user_key)` under
/// a sliding-window-counter model.
///
/// Algorithm:
/// 1. UPSERT the current-window row (atomic single statement) — value += 1.
/// 2. SELECT the current and previous window values.
/// 3. estimated = curr + prev * (1 - elapsed_fraction).
/// 4. If estimated <= max → Granted.
/// 5. Else compensate (UPDATE curr value -= 1) + compute retry_at by
///    inverting the estimate formula, jittered by `job_id`.
pub async fn try_consume_rate_token<C>(
    db: &C,
    table_config: &TableConfig,
    class_name: &str,
    user_key: &str,
    window: chrono::Duration,
    max: i32,
    job_id: i64,
) -> Result<ConsumeResult, DbErr>
where
    C: ConnectionTrait,
{
    let window_secs = window.num_seconds().max(1);
    let now_secs = fetch_server_epoch_seconds(db).await?;
    let window_idx = now_secs.div_euclid(window_secs);
    let prev_idx = window_idx - 1;
    let elapsed_in_window = now_secs - window_idx * window_secs;

    let curr_key = format!("rate:{class_name}/{user_key}:{window_idx}");
    let prev_key = format!("rate:{class_name}/{user_key}:{prev_idx}");

    let window_end_secs = (window_idx + 1) * window_secs;
    // Keep curr row alive one extra window after window_end so the *next*
    // window can read it as `prev` before dispatcher.delete_expired sweeps it.
    let row_expires_secs = (window_idx + 2) * window_secs;
    let expires_at = chrono::DateTime::from_timestamp(row_expires_secs, 0)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| DbErr::Custom("rate-limit: invalid expires_at timestamp".into()))?;
    let now_naive = chrono::DateTime::from_timestamp(now_secs, 0)
        .map(|dt| dt.naive_utc())
        .ok_or_else(|| DbErr::Custom("rate-limit: invalid now timestamp".into()))?;

    upsert_increment(db, table_config, &curr_key, expires_at, now_naive).await?;

    let (curr_value_opt, prev_value_opt) =
        fetch_two_counts(db, table_config, &curr_key, &prev_key).await?;
    let curr_value = curr_value_opt.unwrap_or(0);
    let prev_value = prev_value_opt.unwrap_or(0);

    let elapsed_fraction = (elapsed_in_window as f64) / (window_secs as f64);
    let estimated = (curr_value as f64) + (prev_value as f64) * (1.0 - elapsed_fraction);

    if estimated <= max as f64 {
        return Ok(ConsumeResult::Granted);
    }

    // Over the limit — compensate the speculative increment, then compute when
    // the bucket would naturally drop below `max` if no further consumes happen.
    decrement_unchecked(db, table_config, &curr_key).await?;

    let retry_at = solve_retry_at(
        estimated,
        prev_value,
        max,
        now_secs,
        window_secs,
        window_end_secs,
        job_id,
    )?;
    Ok(ConsumeResult::Throttled { retry_at })
}

/// Invert the sliding-window estimate equation to find the earliest second
/// at which `estimated(t) <= max` (assuming no new consumes).
///
/// When `prev_value == 0` the current window holds all the load and the
/// soonest recovery is window_end (where prev becomes whatever curr is now,
/// but the sliding decay then starts over from there).
fn solve_retry_at(
    estimated: f64,
    prev_value: i32,
    max: i32,
    now_secs: i64,
    window_secs: i64,
    window_end_secs: i64,
    job_id: i64,
) -> Result<NaiveDateTime, DbErr> {
    let base_secs: i64 = if prev_value <= 0 {
        window_end_secs
    } else {
        let needed_decay = (estimated - max as f64).max(0.0);
        let decay_fraction = (needed_decay / prev_value as f64).clamp(0.0, 1.0);
        now_secs + ((window_secs as f64) * decay_fraction).ceil() as i64
    };

    // Deterministic sub-second jitter: spread throttled jobs from the same
    // bucket across the next window using job_id as the seed. Keeps
    // reproductions trivial (same job → same offset) versus stochastic `rand`.
    //
    // Computed in nanoseconds so the smallest supported window (1s) still
    // produces a meaningful spread. With integer-second jitter, every
    // throttled job at window=1s landed at the same exact second, defeating
    // the anti-stampede goal.
    let jitter_nanos = ((job_id.unsigned_abs() % 1000) as i64 * window_secs * 1_000_000_000) / 1000;
    let jitter_duration = chrono::Duration::nanoseconds(jitter_nanos);

    // Clamp base to (now, ...) — clock skew or fast loops can push base into
    // the past before jitter is applied; make sure the dispatcher always has
    // a future scheduled_at to promote against.
    let base_secs = base_secs.max(now_secs + 1);

    chrono::DateTime::from_timestamp(base_secs, 0)
        .map(|dt| dt.naive_utc() + jitter_duration)
        .ok_or_else(|| DbErr::Custom("rate-limit: invalid retry_at timestamp".into()))
}

async fn fetch_server_epoch_seconds<C>(db: &C) -> Result<i64, DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let sql = match backend {
        DatabaseBackend::Postgres => "SELECT EXTRACT(EPOCH FROM NOW())::BIGINT AS now_secs",
        DatabaseBackend::MySql => "SELECT UNIX_TIMESTAMP() AS now_secs",
        DatabaseBackend::Sqlite => "SELECT CAST(strftime('%s','now') AS INTEGER) AS now_secs",
    };
    let row = db
        .query_one(Statement::from_string(backend, sql.to_owned()))
        .await?
        .ok_or_else(|| DbErr::Custom("rate-limit: server clock query returned no rows".into()))?;
    row.try_get::<i64>("", "now_secs")
}

async fn upsert_increment<C>(
    db: &C,
    table_config: &TableConfig,
    key: &str,
    expires_at: NaiveDateTime,
    now: NaiveDateTime,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let table = &table_config.semaphores;
    let sql = match backend {
        DatabaseBackend::Postgres => format!(
            "INSERT INTO {table} (key, value, expires_at, created_at, updated_at) \
             VALUES ($1, 1, $2, $3, $3) \
             ON CONFLICT (key) DO UPDATE SET value = {table}.value + 1, updated_at = $3"
        ),
        DatabaseBackend::Sqlite => format!(
            "INSERT INTO {table} (key, value, expires_at, created_at, updated_at) \
             VALUES (?, 1, ?, ?, ?) \
             ON CONFLICT(key) DO UPDATE SET value = value + 1, updated_at = excluded.updated_at"
        ),
        DatabaseBackend::MySql => format!(
            "INSERT INTO {table} (key, value, expires_at, created_at, updated_at) \
             VALUES (?, 1, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE value = value + 1, updated_at = VALUES(updated_at)"
        ),
    };
    let values: Vec<sea_orm::Value> = match backend {
        DatabaseBackend::Postgres => vec![key.into(), expires_at.into(), now.into()],
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            vec![key.into(), expires_at.into(), now.into(), now.into()]
        }
    };
    db.execute(Statement::from_sql_and_values(backend, sql, values))
        .await?;
    Ok(())
}

async fn fetch_two_counts<C>(
    db: &C,
    table_config: &TableConfig,
    curr_key: &str,
    prev_key: &str,
) -> Result<(Option<i32>, Option<i32>), DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let table = &table_config.semaphores;
    let sql = match backend {
        DatabaseBackend::Postgres => {
            format!("SELECT key, value FROM {table} WHERE key IN ($1, $2)")
        }
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            format!("SELECT key, value FROM {table} WHERE key IN (?, ?)")
        }
    };
    let rows = db
        .query_all(Statement::from_sql_and_values(
            backend,
            sql,
            vec![curr_key.into(), prev_key.into()],
        ))
        .await?;
    let (mut curr, mut prev) = (None, None);
    for row in rows {
        let k: String = row.try_get("", "key")?;
        let v: i32 = row.try_get("", "value")?;
        if k == curr_key {
            curr = Some(v);
        } else if k == prev_key {
            prev = Some(v);
        }
    }
    Ok((curr, prev))
}

/// UPDATE value = value - 1 without bounds check — only used by the rate-limit
/// compensating path immediately after a speculative UPSERT increment.
async fn decrement_unchecked<C>(db: &C, table_config: &TableConfig, key: &str) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let table = &table_config.semaphores;
    let sql = match backend {
        DatabaseBackend::Postgres => format!("UPDATE {table} SET value = value - 1 WHERE key = $1"),
        DatabaseBackend::Sqlite | DatabaseBackend::MySql => {
            format!("UPDATE {table} SET value = value - 1 WHERE key = ?")
        }
    };
    db.execute(Statement::from_sql_and_values(
        backend,
        sql,
        vec![key.into()],
    ))
    .await?;
    Ok(())
}
