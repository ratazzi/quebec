use crate::context::{ConcurrencyConstraint, TableConfig};
use crate::dialect::Dialect;
use crate::query_builder::quote_identifier;
use chrono::NaiveDateTime;
use sea_orm::{ConnectionTrait, DatabaseBackend, DbErr, Statement};

/// How long taking a slot may take before the semaphore's expiry is written
/// again once the slot is ours: a tenth of the period, at most a second.
///
/// An uncontended acquire takes a few milliseconds and skips the extra
/// write; any wait for a row lock long enough to eat into the period
/// triggers it, including with a sub-second period.
fn restamp_after(duration: chrono::Duration) -> std::time::Duration {
    (duration / 10)
        .to_std()
        .unwrap_or_default()
        .min(std::time::Duration::from_secs(1))
}

/// Take a slot on the semaphore for `key`, Solid Queue's `Semaphore.wait`.
///
/// Returns the id of the semaphore the slot was taken from, or `None` when no
/// slot is free. Callers acquiring a job's concurrency slot record that id
/// (see [`crate::concurrency_lock`]) so the job later gives the slot back to
/// this very semaphore, not to whichever one holds the key by then.
///
/// The expiry written with the slot is computed before any statement that
/// may wait for another transaction's row lock. Solid Queue computes it only
/// once it holds the lock. So when taking the slot took a while, the expiry
/// is written again from the current time: otherwise a wait as long as
/// `duration` leaves a semaphore that is already expired when it commits,
/// and concurrency maintenance deletes it under its holder.
pub async fn acquire_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    concurrency_limit: i32,
    duration: chrono::Duration,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    let started = std::time::Instant::now();
    let acquired = take_slot(db, table_config, key, concurrency_limit, duration).await?;
    if let Some(id) = acquired.filter(|_| started.elapsed() >= restamp_after(duration)) {
        restamp_expiry(db, table_config, id, duration).await?;
    }
    Ok(acquired)
}

/// Set the expiry of semaphore `id`, on which this transaction just took a
/// slot (so it already holds the row lock), to `duration` from now, as
/// Solid Queue's `attempt_decrement` does once it holds the lock.
async fn restamp_expiry<C>(
    db: &C,
    table_config: &TableConfig,
    id: i64,
    duration: chrono::Duration,
) -> Result<(), DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let now = chrono::Utc::now().naive_utc();
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => format!(
            "UPDATE {} SET expires_at = $1, updated_at = $2 WHERE id = $3",
            table_config.semaphores
        ),
        Dialect::MySql | Dialect::Sqlite => format!(
            "UPDATE {} SET expires_at = ?, updated_at = ? WHERE id = ?",
            table_config.semaphores
        ),
    };
    db.execute_raw(Statement::from_sql_and_values(
        backend,
        sql,
        vec![(now + duration).into(), now.into(), id.into()],
    ))
    .await?;
    Ok(())
}

async fn take_slot<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    concurrency_limit: i32,
    duration: chrono::Duration,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration;
    let key_col = quote_identifier(db.get_database_backend(), "key");

    // Read before writing, like Solid Queue's `Semaphore::Proxy#wait`. An open
    // semaphore is taken with the conditional UPDATE below, which locks the
    // row anyway; only a missing key falls through to the creation path.
    //
    // A semaphore found full, or whose last slot another transaction took
    // between our read and our UPDATE, is decided under a row lock (Solid
    // Queue reads it with `lock` outright, since #456): see
    // [`decide_under_lock`]. The lock is taken only then, so open keys stay a
    // single UPDATE.
    let found = find_value(db, table_config, &key, false).await?;
    if found.is_some_and(|value| value > 0)
        && attempt_decrement(db, table_config, key.clone(), expires_at, now).await?
    {
        return acquired_semaphore_id(db, table_config, &key).await;
    }
    if found.is_some() {
        if let Some(decided) = decide_under_lock(db, table_config, &key, expires_at, now).await? {
            return Ok(decided);
        }
        // Expired and deleted since our first read: create it below.
    }

    if db.get_database_backend() == DatabaseBackend::MySql {
        // INSERT IGNORE takes a shared lock on an existing key. Concurrent
        // transactions then deadlock when both try to upgrade it in the
        // following UPDATE. The duplicate-key UPDATE takes an exclusive lock
        // directly. Assign value last: MySQL evaluates assignments in order.
        let sql = format!(
            "INSERT INTO {table} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
             expires_at = IF({table}.value > 0, ?, {table}.expires_at), \
             updated_at = IF({table}.value > 0, ?, {table}.updated_at), \
             value = IF({table}.value > 0, {table}.value - 1, {table}.value)",
            table = table_config.semaphores
        );
        let result = db
            .execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::MySql,
                sql,
                vec![
                    key.clone().into(),
                    (concurrency_limit - 1).into(),
                    expires_at.into(),
                    now.into(),
                    now.into(),
                    expires_at.into(),
                    now.into(),
                ],
            ))
            .await?;
        // SQLx enables CLIENT_FOUND_ROWS, so a no-op duplicate reports one
        // affected row just like an INSERT. With this table's auto-increment
        // ID, MySQL returns zero insert ID only for the unchanged full row.
        return id_if_acquired(result.last_insert_id() != 0, db, table_config, &key).await;
    }

    // First, try to create a new semaphore (attempt_creation)
    let create_sql = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            format!(
                "INSERT INTO {} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT ({key_col}) DO NOTHING",
                table_config.semaphores
            )
        }
        Dialect::Sqlite => {
            format!(
                "INSERT OR IGNORE INTO {} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?)",
                table_config.semaphores
            )
        }
        Dialect::MySql => unreachable!(),
    };

    let create_result = db
        .execute_raw(Statement::from_sql_and_values(
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
        return acquired_semaphore_id(db, table_config, &key).await;
    }

    // Semaphore already exists, try to decrement if value > 0. The `WHERE
    // value > 0` predicate naturally covers limit == 1 too: a freed slot has
    // value = limit (= 1), which satisfies > 0, so decrement-to-0 atomically
    // re-acquires. The old `if concurrency_limit == 1 { return false; }`
    // short-circuit here mistook a row's existence for "held" and forced
    // callers to wait for `expires_at` to elapse before the dispatcher's
    // delete_expired sweep cleared the row — turning concurrency_duration
    // into a minimum cooldown rather than a crash-safety TTL.
    if attempt_decrement(db, table_config, key.clone(), expires_at, now).await? {
        return acquired_semaphore_id(db, table_config, &key).await;
    }
    // Full: decide under the row lock, as above. The row exists (we just
    // conflicted with it), so a missing one now gives no slot either.
    Ok(decide_under_lock(db, table_config, &key, expires_at, now)
        .await?
        .flatten())
}

/// Read the semaphore for `key` again with a row lock and take a slot if one
/// is free; `None` when the row no longer exists.
///
/// The caller holds the lock until its transaction ends. When no slot is
/// free, its blocked execution therefore commits before any worker giving a
/// slot back can look for blocked jobs, so that worker sees and promotes it.
/// Without the lock, a slot freed between an unlocked read (or a decrement
/// that matched nothing) and the caller's commit leaves the new job stranded
/// until its blocked row expires and the dispatcher's concurrency
/// maintenance comes round.
async fn decide_under_lock<C>(
    db: &C,
    table_config: &TableConfig,
    key: &str,
    expires_at: NaiveDateTime,
    now: NaiveDateTime,
) -> Result<Option<Option<i64>>, DbErr>
where
    C: ConnectionTrait,
{
    match find_value(db, table_config, key, true).await? {
        Some(value) if value > 0 => {
            let acquired =
                attempt_decrement(db, table_config, key.to_string(), expires_at, now).await?;
            id_if_acquired(acquired, db, table_config, key)
                .await
                .map(Some)
        }
        Some(_) => Ok(Some(None)),
        None => Ok(None),
    }
}

async fn id_if_acquired<C>(
    acquired: bool,
    db: &C,
    table_config: &TableConfig,
    key: &str,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    if acquired {
        acquired_semaphore_id(db, table_config, key).await
    } else {
        Ok(None)
    }
}

/// Id of the semaphore for `key` that this transaction just took a slot on.
///
/// A locking read, like Solid Queue's `acquired_semaphore_id`: on MySQL with
/// REPEATABLE READ a plain read uses the transaction's snapshot, which can
/// still show a semaphore that expired and was deleted after the snapshot was
/// taken, instead of the one just created. `FOR UPDATE` reads the latest
/// committed row, and the row is already locked by our own write. SQLite has
/// no row locks and no stale snapshot inside its write transaction.
async fn acquired_semaphore_id<C>(
    db: &C,
    table_config: &TableConfig,
    key: &str,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let key_col = quote_identifier(backend, "key");
    let table = &table_config.semaphores;
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => {
            format!("SELECT id FROM {table} WHERE {key_col} = $1 FOR UPDATE")
        }
        Dialect::MySql => format!("SELECT id FROM {table} WHERE {key_col} = ? FOR UPDATE"),
        Dialect::Sqlite => format!("SELECT id FROM {table} WHERE {key_col} = ?"),
    };
    db.query_one_raw(Statement::from_sql_and_values(
        backend,
        sql,
        vec![key.to_string().into()],
    ))
    .await?
    .map(|row| row.try_get::<i64>("", "id"))
    .transpose()
}

/// Current value of the semaphore row for `key`, if it exists. With `lock`,
/// a locking read that holds the row until the transaction ends (SQLite has
/// no row locks; its write transaction already serializes writers).
async fn find_value<C>(
    db: &C,
    table_config: &TableConfig,
    key: &str,
    lock: bool,
) -> Result<Option<i32>, DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let key_col = quote_identifier(backend, "key");
    let placeholder = match Dialect::from(backend) {
        Dialect::Postgres => "$1",
        Dialect::Sqlite | Dialect::MySql => "?",
    };
    let lock_clause = if lock && backend != DatabaseBackend::Sqlite {
        " FOR UPDATE"
    } else {
        ""
    };
    let sql = format!(
        "SELECT value FROM {} WHERE {key_col} = {placeholder}{lock_clause}",
        table_config.semaphores
    );
    db.query_one_raw(Statement::from_sql_and_values(
        backend,
        sql,
        vec![key.to_string().into()],
    ))
    .await?
    .map(|row| row.try_get::<i32>("", "value"))
    .transpose()
}

/// Take one slot from an existing semaphore row if one is free.
async fn attempt_decrement<C>(
    db: &C,
    table_config: &TableConfig,
    key: String,
    expires_at: NaiveDateTime,
    now: NaiveDateTime,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let key_col = quote_identifier(db.get_database_backend(), "key");
    let decrement_sql = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE {key_col} = $1 AND value > 0",
                table_config.semaphores
            )
        }
        Dialect::Sqlite => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE {key_col} = ? AND value > 0",
                table_config.semaphores
            )
        }
        Dialect::MySql => {
            format!(
                "UPDATE {} SET \
             value = value - 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE {key_col} = ? AND value > 0",
                table_config.semaphores
            )
        }
    };

    // Note: Parameter order differs between Postgres ($1,$2,$3) and SQLite/MySQL (? ? ?)
    let decrement_values = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now
            vec![key.into(), expires_at.into(), now.into()]
        }
        Dialect::Sqlite | Dialect::MySql => {
            // SQLite/MySQL: ?=expires_at, ?=updated_at, ?=key (order in SQL)
            vec![expires_at.into(), now.into(), key.into()]
        }
    };

    let decrement_result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            decrement_sql,
            decrement_values,
        ))
        .await?;

    Ok(decrement_result.rows_affected() > 0)
}

/// Re-acquire a MySQL semaphore slot that was just released in this transaction.
/// The successful release already locked an existing row. An UPDATE avoids
/// the auto-increment PRIMARY insert-intention lock taken by the generic UPSERT
/// while we hold the blocked-execution ordering-index lock.
pub async fn reacquire_released_semaphore<C>(
    db: &C,
    table_config: &TableConfig,
    key: &str,
    duration: chrono::Duration,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    if db.get_database_backend() != DatabaseBackend::MySql {
        return Err(DbErr::Custom(
            "reacquire_released_semaphore requires MySQL".into(),
        ));
    }
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration;
    let key_col = quote_identifier(DatabaseBackend::MySql, "key");
    let sql = format!(
        "UPDATE {} SET value = value - 1, expires_at = ?, updated_at = ? \
         WHERE {key_col} = ? AND value > 0",
        table_config.semaphores
    );
    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::MySql,
            sql,
            vec![expires_at.into(), now.into(), key.to_string().into()],
        ))
        .await?;
    id_if_acquired(result.rows_affected() > 0, db, table_config, key).await
}

/// Convenience function to acquire semaphore using ConcurrencyConstraint.
/// `default_duration` applies when the constraint sets no duration
/// (`AppContext::concurrency_period`).
pub async fn acquire_semaphore_with_constraint<C>(
    db: &C,
    table_config: &TableConfig,
    constraint: &ConcurrencyConstraint,
    default_duration: chrono::Duration,
) -> Result<Option<i64>, DbErr>
where
    C: ConnectionTrait,
{
    acquire_semaphore(
        db,
        table_config,
        constraint.key.clone(),
        constraint.limit,
        constraint.duration.unwrap_or(default_duration),
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
    duration: chrono::Duration,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration;
    let key_col = quote_identifier(db.get_database_backend(), "key");

    // Try to increment the semaphore value if below max (limit)
    // value ranges from 0 to limit, where limit means all slots are free
    // This matches Solid Queue's: Semaphore.where(key: key, value: ...limit).update_all(...)
    let increment_sql = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = $2, \
             updated_at = $3 \
             WHERE {key_col} = $1 AND value < $4",
                table_config.semaphores
            )
        }
        Dialect::Sqlite | Dialect::MySql => {
            format!(
                "UPDATE {} SET \
             value = value + 1, \
             expires_at = ?, \
             updated_at = ? \
             WHERE {key_col} = ? AND value < ?",
                table_config.semaphores
            )
        }
    };

    // Note: Parameter order differs between Postgres ($1,$2,$3,$4) and SQLite/MySQL (? ? ? ?)
    // Postgres uses positional params, SQLite/MySQL uses sequential order of appearance
    let increment_values = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now, $4=limit
            vec![
                key.clone().into(),
                expires_at.into(),
                now.into(),
                limit.into(),
            ]
        }
        Dialect::Sqlite | Dialect::MySql => {
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
        .execute_raw(Statement::from_sql_and_values(
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
    let update_expires_sql = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            format!(
                "UPDATE {} SET \
             expires_at = $2, \
             updated_at = $3 \
             WHERE {key_col} = $1",
                table_config.semaphores
            )
        }
        Dialect::Sqlite | Dialect::MySql => {
            format!(
                "UPDATE {} SET \
             expires_at = ?, \
             updated_at = ? \
             WHERE {key_col} = ?",
                table_config.semaphores
            )
        }
    };

    // Same parameter order issue as above
    let update_values = match Dialect::from(db.get_database_backend()) {
        Dialect::Postgres => {
            // Postgres: $1=key, $2=expires_at, $3=now
            vec![key.into(), expires_at.into(), now.into()]
        }
        Dialect::Sqlite | Dialect::MySql => {
            // SQLite/MySQL: ?=expires_at, ?=updated_at, ?=key (order in SQL)
            vec![expires_at.into(), now.into(), key.into()]
        }
    };

    let update_result = db
        .execute_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            update_expires_sql,
            update_values,
        ))
        .await?;

    Ok(update_result.rows_affected() > 0)
}

/// Give a slot back to the semaphore with this `id`, the one the job took it
/// from: Solid Queue's `attempt_increment` on `semaphore_to_release`.
///
/// If that semaphore expired and was deleted, a semaphore now holding the same
/// key belongs to other jobs, so nothing is given back and this returns false.
/// Otherwise behaves like [`release_semaphore`].
pub async fn release_semaphore_by_id<C>(
    db: &C,
    table_config: &TableConfig,
    id: i64,
    limit: i32,
    duration: chrono::Duration,
) -> Result<bool, DbErr>
where
    C: ConnectionTrait,
{
    let backend = db.get_database_backend();
    let now = chrono::Utc::now().naive_utc();
    let expires_at = now + duration;
    let table = &table_config.semaphores;
    let (increment_sql, touch_sql) = match Dialect::from(backend) {
        Dialect::Postgres => (
            format!(
                "UPDATE {table} SET value = value + 1, expires_at = $1, updated_at = $2 \
                 WHERE id = $3 AND value < $4"
            ),
            format!("UPDATE {table} SET expires_at = $1, updated_at = $2 WHERE id = $3"),
        ),
        Dialect::Sqlite | Dialect::MySql => (
            format!(
                "UPDATE {table} SET value = value + 1, expires_at = ?, updated_at = ? \
                 WHERE id = ? AND value < ?"
            ),
            format!("UPDATE {table} SET expires_at = ?, updated_at = ? WHERE id = ?"),
        ),
    };

    let incremented = db
        .execute_raw(Statement::from_sql_and_values(
            backend,
            increment_sql,
            vec![expires_at.into(), now.into(), id.into(), limit.into()],
        ))
        .await?
        .rows_affected();
    if incremented > 0 {
        return Ok(true);
    }

    // Already at the limit: keep the row alive, as release_semaphore does.
    let touched = db
        .execute_raw(Statement::from_sql_and_values(
            backend,
            touch_sql,
            vec![expires_at.into(), now.into(), id.into()],
        ))
        .await?
        .rows_affected();
    Ok(touched > 0)
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
    // We split into whole seconds + sub-second nanoseconds *before* scaling
    // up to nanoseconds. The earlier `seed * window_secs * 1e9 / 1000` form
    // overflowed i64 once `window_secs > ~9.2M` (~107d), so a legal
    // `timedelta(days=365)` window panicked on overflow at throttle time.
    // Splitting keeps every intermediate product within i64 for the full
    // supported window range (≤ i32::MAX seconds).
    let seed = (job_id.unsigned_abs() % 1000) as i64;
    let scaled = seed * window_secs; // max ~999 * 2.1e9 = ~2.1e12, well within i64
    let jitter_whole_secs = scaled / 1000;
    let jitter_sub_nanos = (scaled % 1000) * 1_000_000;
    let jitter_duration = chrono::Duration::seconds(jitter_whole_secs)
        + chrono::Duration::nanoseconds(jitter_sub_nanos);

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
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => "SELECT EXTRACT(EPOCH FROM NOW())::BIGINT AS now_secs",
        Dialect::MySql => "SELECT UNIX_TIMESTAMP() AS now_secs",
        Dialect::Sqlite => "SELECT CAST(strftime('%s','now') AS INTEGER) AS now_secs",
    };
    let row = db
        .query_one_raw(Statement::from_string(backend, sql.to_owned()))
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
    let key_col = quote_identifier(backend, "key");
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => format!(
            "INSERT INTO {table} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES ($1, 1, $2, $3, $3) \
             ON CONFLICT ({key_col}) DO UPDATE SET value = {table}.value + 1, updated_at = $3"
        ),
        Dialect::Sqlite => format!(
            "INSERT INTO {table} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES (?, 1, ?, ?, ?) \
             ON CONFLICT({key_col}) DO UPDATE SET value = value + 1, updated_at = excluded.updated_at"
        ),
        Dialect::MySql => format!(
            "INSERT INTO {table} ({key_col}, value, expires_at, created_at, updated_at) \
             VALUES (?, 1, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE value = value + 1, updated_at = VALUES(updated_at)"
        ),
    };
    let values: Vec<sea_orm::Value> = match Dialect::from(backend) {
        Dialect::Postgres => vec![key.into(), expires_at.into(), now.into()],
        Dialect::Sqlite | Dialect::MySql => {
            vec![key.into(), expires_at.into(), now.into(), now.into()]
        }
    };
    db.execute_raw(Statement::from_sql_and_values(backend, sql, values))
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
    let key_col = quote_identifier(backend, "key");
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => {
            format!("SELECT {key_col}, value FROM {table} WHERE {key_col} IN ($1, $2)")
        }
        Dialect::Sqlite | Dialect::MySql => {
            format!("SELECT {key_col}, value FROM {table} WHERE {key_col} IN (?, ?)")
        }
    };
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
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
    let key_col = quote_identifier(backend, "key");
    let sql = match Dialect::from(backend) {
        Dialect::Postgres => {
            format!("UPDATE {table} SET value = value - 1 WHERE {key_col} = $1")
        }
        Dialect::Sqlite | Dialect::MySql => {
            format!("UPDATE {table} SET value = value - 1 WHERE {key_col} = ?")
        }
    };
    db.execute_raw(Statement::from_sql_and_values(
        backend,
        sql,
        vec![key.into()],
    ))
    .await?;
    Ok(())
}
