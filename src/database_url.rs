use std::fmt;

use url::Url;

use crate::error::{QuebecError, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseKind {
    Postgres,
    Mysql,
    Sqlite,
    Unknown,
}

impl DatabaseKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseKind::Postgres => "PostgreSQL",
            DatabaseKind::Mysql => "MySQL",
            DatabaseKind::Sqlite => "SQLite",
            DatabaseKind::Unknown => "Unknown",
        }
    }
}

/// Database connection URL with SQLAlchemy-compatible normalization.
///
/// `Display` yields a masked form (password redacted, query/fragment stripped)
/// safe to log or show in the control plane.
#[derive(Debug, Clone)]
pub struct DatabaseUrl {
    raw: String,
    parsed: Url,
    kind: DatabaseKind,
}

impl DatabaseUrl {
    pub fn parse(input: &str) -> Result<Self> {
        let normalized = normalize(input);
        let parsed = Url::parse(&normalized)?;
        let kind = match parsed.scheme() {
            s if s.starts_with("postgres") => DatabaseKind::Postgres,
            s if s.starts_with("mysql") => DatabaseKind::Mysql,
            s if s.starts_with("sqlite") => DatabaseKind::Sqlite,
            _ => DatabaseKind::Unknown,
        };
        Ok(Self {
            raw: normalized,
            parsed,
            kind,
        })
    }

    /// Connection string in the form sqlx / SeaORM accept.
    pub fn as_connect_str(&self) -> &str {
        &self.raw
    }

    pub fn url(&self) -> &Url {
        &self.parsed
    }

    pub fn kind(&self) -> DatabaseKind {
        self.kind
    }

    pub fn is_postgres(&self) -> bool {
        matches!(self.kind, DatabaseKind::Postgres)
    }

    pub fn is_sqlite(&self) -> bool {
        matches!(self.kind, DatabaseKind::Sqlite)
    }

    pub fn is_mysql(&self) -> bool {
        matches!(self.kind, DatabaseKind::Mysql)
    }

    /// Password redacted, query/fragment stripped.
    pub fn masked(&self) -> String {
        let mut u = self.parsed.clone();
        if u.password().is_some() {
            let _ = u.set_password(Some("***"));
        }
        u.set_query(None);
        u.set_fragment(None);
        u.to_string()
    }
}

impl fmt::Display for DatabaseUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.masked())
    }
}

impl std::str::FromStr for DatabaseUrl {
    type Err = QuebecError;

    fn from_str(s: &str) -> Result<Self> {
        Self::parse(s)
    }
}

fn normalize(input: &str) -> String {
    let stripped = strip_driver_suffix(input);
    normalize_sqlite(&stripped)
}

/// `postgresql+psycopg://...` → `postgresql://...`
fn strip_driver_suffix(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let scheme = &url[..scheme_end];
    let Some(plus) = scheme.find('+') else {
        return url.to_string();
    };
    format!("{}{}", &scheme[..plus], &url[scheme_end..])
}

/// Handle unambiguous SQLAlchemy sqlite quirks:
///   - `sqlite:///:memory:`   → `sqlite::memory:`
///   - `sqlite:////abs/path`  → `sqlite:///abs/path`
/// The 3-slash `sqlite:///relative.db` form is ambiguous (SQLAlchemy: relative,
/// sqlx: absolute) and is left untouched — sqlx semantics win.
fn normalize_sqlite(url: &str) -> String {
    let Some(rest) = url.strip_prefix("sqlite://") else {
        return url.to_string();
    };
    if rest == "/:memory:" {
        return "sqlite::memory:".to_string();
    }
    if let Some(abs) = rest.strip_prefix("//") {
        return format!("sqlite:///{}", abs);
    }
    url.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_sqlalchemy_driver_suffix() {
        assert_eq!(
            DatabaseUrl::parse("postgresql+psycopg://u:p@h/db")
                .unwrap()
                .as_connect_str(),
            "postgresql://u:p@h/db"
        );
        assert_eq!(
            DatabaseUrl::parse("postgresql+asyncpg://h/db")
                .unwrap()
                .as_connect_str(),
            "postgresql://h/db"
        );
        assert_eq!(
            DatabaseUrl::parse("mysql+pymysql://u@h/db")
                .unwrap()
                .as_connect_str(),
            "mysql://u@h/db"
        );
        assert_eq!(
            DatabaseUrl::parse("sqlite+aiosqlite:///tmp/x.db")
                .unwrap()
                .as_connect_str(),
            "sqlite:///tmp/x.db"
        );
    }

    #[test]
    fn normalizes_sqlite_memory() {
        assert_eq!(
            DatabaseUrl::parse("sqlite:///:memory:")
                .unwrap()
                .as_connect_str(),
            "sqlite::memory:"
        );
    }

    #[test]
    fn normalizes_sqlite_four_slash_abs() {
        assert_eq!(
            DatabaseUrl::parse("sqlite:////var/lib/q.db")
                .unwrap()
                .as_connect_str(),
            "sqlite:///var/lib/q.db"
        );
    }

    #[test]
    fn leaves_non_sqlalchemy_urls_alone() {
        assert_eq!(
            DatabaseUrl::parse("postgres://u:p@h/db")
                .unwrap()
                .as_connect_str(),
            "postgres://u:p@h/db"
        );
        assert_eq!(
            DatabaseUrl::parse("sqlite:///var/x.db")
                .unwrap()
                .as_connect_str(),
            "sqlite:///var/x.db"
        );
    }

    #[test]
    fn detects_kind() {
        assert!(DatabaseUrl::parse("postgres://h/d").unwrap().is_postgres());
        assert!(DatabaseUrl::parse("postgresql://h/d")
            .unwrap()
            .is_postgres());
        assert!(DatabaseUrl::parse("mysql://h/d").unwrap().is_mysql());
        assert!(DatabaseUrl::parse("sqlite::memory:").unwrap().is_sqlite());
    }

    #[test]
    fn masked_strips_password_and_query() {
        let url = DatabaseUrl::parse("postgres://alice:secret@h:5432/db?sslmode=require").unwrap();
        assert_eq!(url.masked(), "postgres://alice:***@h:5432/db");
        assert_eq!(url.to_string(), "postgres://alice:***@h:5432/db");
    }
}
