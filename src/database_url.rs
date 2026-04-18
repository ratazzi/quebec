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

    /// Upsert libpq-style SSL query parameters into the DSN.
    ///
    /// Each provided value replaces an existing query param of the same name
    /// *and any accepted alias*; `None` leaves the existing value (if any)
    /// untouched. Param order is preserved for non-SSL pairs. sqlx-postgres
    /// accepts both canonical (`sslmode`, `sslrootcert`, `sslcert`, `sslkey`)
    /// and aliased (`ssl-mode`, `ssl-root-cert`, `ssl-ca`, `ssl-cert`,
    /// `ssl-key`) spellings — all are scrubbed when the matching override is
    /// supplied so the caller's value is the only effective one.
    pub fn with_ssl_params(
        &self,
        sslmode: Option<&str>,
        sslrootcert: Option<&str>,
        sslcert: Option<&str>,
        sslkey: Option<&str>,
    ) -> Result<Self> {
        let overrides: &[(&str, Option<&str>)] = &[
            ("sslmode", sslmode),
            ("sslrootcert", sslrootcert),
            ("sslcert", sslcert),
            ("sslkey", sslkey),
        ];
        if overrides.iter().all(|(_, v)| v.is_none()) {
            return Ok(self.clone());
        }

        let existing: Vec<(String, String)> = self
            .parsed
            .query_pairs()
            .filter(|(k, _)| {
                !overrides.iter().any(|(name, val)| {
                    val.is_some() && ssl_param_aliases(name).iter().any(|a| *a == k.as_ref())
                })
            })
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        let mut u = self.parsed.clone();
        {
            let mut qp = u.query_pairs_mut();
            qp.clear();
            for (k, v) in &existing {
                qp.append_pair(k, v);
            }
            for (name, val) in overrides {
                if let Some(v) = val {
                    qp.append_pair(name, v);
                }
            }
        }
        if u.query().is_some_and(|q| q.is_empty()) {
            u.set_query(None);
        }
        Self::parse(u.as_str())
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

/// Aliases sqlx-postgres accepts for a given canonical libpq SSL query param.
/// See `sqlx-postgres` `options/parse.rs` `parse_from_url`.
pub fn ssl_param_aliases(canonical: &str) -> &'static [&'static str] {
    match canonical {
        "sslmode" => &["sslmode", "ssl-mode"],
        "sslrootcert" => &["sslrootcert", "ssl-root-cert", "ssl-ca"],
        "sslcert" => &["sslcert", "ssl-cert"],
        "sslkey" => &["sslkey", "ssl-key"],
        _ => &[],
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

    #[test]
    fn ssl_params_all_none_returns_clone() {
        let url = DatabaseUrl::parse("postgres://h/db?application_name=x").unwrap();
        let out = url.with_ssl_params(None, None, None, None).unwrap();
        assert_eq!(out.as_connect_str(), "postgres://h/db?application_name=x");
    }

    #[test]
    fn ssl_params_inserts_into_empty_query() {
        let url = DatabaseUrl::parse("postgres://h/db").unwrap();
        let out = url
            .with_ssl_params(Some("require"), None, None, None)
            .unwrap();
        assert_eq!(out.as_connect_str(), "postgres://h/db?sslmode=require");
    }

    #[test]
    fn ssl_params_override_existing_sslmode() {
        let url = DatabaseUrl::parse("postgres://h/db?sslmode=disable&application_name=x").unwrap();
        let out = url
            .with_ssl_params(Some("require"), None, None, None)
            .unwrap();
        // Existing non-ssl params preserved; sslmode replaced.
        assert!(out.as_connect_str().contains("application_name=x"));
        assert!(out.as_connect_str().contains("sslmode=require"));
        assert!(!out.as_connect_str().contains("sslmode=disable"));
    }

    #[test]
    fn ssl_params_leaves_existing_when_override_is_none() {
        let url = DatabaseUrl::parse("postgres://h/db?sslmode=require").unwrap();
        let out = url
            .with_ssl_params(None, Some("/etc/ca.pem"), None, None)
            .unwrap();
        assert!(out.as_connect_str().contains("sslmode=require"));
        assert!(out.as_connect_str().contains("sslrootcert=%2Fetc%2Fca.pem"));
    }

    #[test]
    fn ssl_params_override_strips_sqlx_aliases() {
        // sqlx accepts `ssl-mode` as alias; overriding `sslmode` should also
        // strip the alias form so the caller's value is the only effective one.
        let url = DatabaseUrl::parse("postgres://h/db?ssl-mode=allow").unwrap();
        let out = url
            .with_ssl_params(Some("require"), None, None, None)
            .unwrap();
        assert!(out.as_connect_str().contains("sslmode=require"));
        assert!(!out.as_connect_str().contains("ssl-mode"));
        assert!(!out.as_connect_str().contains("allow"));
    }

    #[test]
    fn ssl_params_override_strips_sqlx_rootcert_aliases() {
        // sqlx accepts `ssl-ca` and `ssl-root-cert` as aliases of sslrootcert.
        let url =
            DatabaseUrl::parse("postgres://h/db?ssl-ca=/old/ca.pem&ssl-root-cert=/older/ca.pem")
                .unwrap();
        let out = url
            .with_ssl_params(None, Some("/new/ca.pem"), None, None)
            .unwrap();
        assert!(out.as_connect_str().contains("sslrootcert=%2Fnew%2Fca.pem"));
        assert!(!out.as_connect_str().contains("ssl-ca"));
        assert!(!out.as_connect_str().contains("ssl-root-cert"));
    }

    #[test]
    fn ssl_params_percent_encodes_paths() {
        let url = DatabaseUrl::parse("postgres://h/db").unwrap();
        let out = url
            .with_ssl_params(
                Some("verify-full"),
                Some("/path/with space/ca.pem"),
                None,
                None,
            )
            .unwrap();
        assert!(out.as_connect_str().contains("sslmode=verify-full"));
        assert!(out
            .as_connect_str()
            .contains("sslrootcert=%2Fpath%2Fwith+space%2Fca.pem"));
    }
}
