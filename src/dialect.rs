//! Exhaustive view of the SQL dialects Quebec supports.
//!
//! sea-orm 2 marks `DbBackend` `#[non_exhaustive]`, so every `match` on it
//! needs a wildcard arm. Matching on [`Dialect`] instead keeps per-backend SQL
//! exhaustive: an unknown backend is rejected here, once, rather than silently
//! falling into a `_` arm at each call site.

use sea_orm::DbBackend;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    MySql,
    Postgres,
    Sqlite,
}

impl From<DbBackend> for Dialect {
    fn from(backend: DbBackend) -> Self {
        match backend {
            DbBackend::MySql => Self::MySql,
            DbBackend::Postgres => Self::Postgres,
            DbBackend::Sqlite => Self::Sqlite,
            other => unreachable!("unsupported database backend: {other:?}"),
        }
    }
}
