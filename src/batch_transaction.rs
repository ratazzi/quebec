//! A Python context's enqueue transaction and its after-commit work.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use sea_orm::{DatabaseTransaction, DbErr};

use crate::context::AppContext;
use crate::error::Result;

#[pyclass(name = "_BatchTransaction")]
pub struct BatchTransaction {
    pub state: Arc<TransactionState>,
}

pub struct TransactionState {
    pub ctx: Arc<AppContext>,
    transaction: Mutex<Option<Arc<DatabaseTransaction>>>,
    starts: Mutex<Vec<i64>>,
    queues: Mutex<HashSet<String>>,
    released: Mutex<Vec<i64>>,
}

type FinishFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;

struct BatchContextVariables {
    transaction: Py<PyAny>,
    batch_id: Py<PyAny>,
}

static BATCH_CONTEXT: PyOnceLock<BatchContextVariables> = PyOnceLock::new();

fn context_variables(py: Python<'_>) -> PyResult<Option<&'static BatchContextVariables>> {
    if let Some(variables) = BATCH_CONTEXT.get(py) {
        return Ok(Some(variables));
    }
    // Native-extension embedding may omit the Python package entirely.
    let Ok(module) = py.import("quebec.context") else {
        return Ok(None);
    };
    BATCH_CONTEXT
        .get_or_try_init(py, || {
            Ok(BatchContextVariables {
                transaction: module.getattr("current_batch_transaction")?.unbind(),
                batch_id: module.getattr("current_batch_id")?.unbind(),
            })
        })
        .map(Some)
}

impl TransactionState {
    pub fn new(ctx: Arc<AppContext>, transaction: DatabaseTransaction) -> Arc<Self> {
        Arc::new(Self {
            ctx,
            transaction: Mutex::new(Some(Arc::new(transaction))),
            starts: Mutex::new(Vec::new()),
            queues: Mutex::new(HashSet::new()),
            released: Mutex::new(Vec::new()),
        })
    }

    pub fn connection(&self) -> std::result::Result<Arc<DatabaseTransaction>, DbErr> {
        self.transaction
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .ok_or_else(|| DbErr::Custom("Batch transaction is already closed".into()))
    }

    pub fn start_after_commit(&self, id: i64) {
        let mut starts = self.starts.lock().unwrap_or_else(|e| e.into_inner());
        if !starts.contains(&id) {
            starts.push(id);
        }
    }

    pub fn after_enqueue(
        &self,
        queues: impl IntoIterator<Item = String>,
        released: impl IntoIterator<Item = i64>,
    ) {
        self.queues
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(queues);
        self.released
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .extend(released);
    }

    pub fn finish(self: Arc<Self>, commit: bool) -> FinishFuture {
        Box::pin(async move {
            let transaction = self
                .transaction
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
                .ok_or_else(|| DbErr::Custom("Batch transaction is already closed".into()))?;
            let transaction = Arc::try_unwrap(transaction).map_err(|_| {
                DbErr::Custom(
                    "Batch transaction closed without commit: enqueues are still using its \
                     connection. It will roll back when they release it and cannot be reused."
                        .into(),
                )
            })?;
            if !commit {
                transaction.rollback().await?;
                return Ok(());
            }
            transaction.commit().await?;
            let db = self.ctx.get_db().await?;
            let queues =
                std::mem::take(&mut *self.queues.lock().unwrap_or_else(|e| e.into_inner()));
            for queue in queues {
                if crate::notify::should_send_notify(&self.ctx, &queue) {
                    if let Err(e) =
                        crate::notify::NotifyManager::send_notify(&self.ctx.name, &*db, &queue)
                            .await
                    {
                        tracing::warn!("Failed to send batch NOTIFY: {e}");
                    }
                }
            }
            let starts =
                std::mem::take(&mut *self.starts.lock().unwrap_or_else(|e| e.into_inner()));
            let mut errors = Vec::new();
            for id in starts {
                if let Err(error) = crate::batch::start(&self.ctx, &db, id).await {
                    errors.push(format!("start batch {id}: {error}"));
                }
            }
            let released =
                std::mem::take(&mut *self.released.lock().unwrap_or_else(|e| e.into_inner()));
            let mut seen = HashSet::new();
            for id in released {
                if seen.insert(id) {
                    if let Err(error) = crate::batch::try_finish(&self.ctx, &db, id).await {
                        errors.push(format!("finish batch {id}: {error}"));
                    }
                }
            }
            if !errors.is_empty() {
                return Err(crate::error::QuebecError::Runtime(format!(
                    "Batch transaction committed, but after-commit work failed: {}",
                    errors.join("; ")
                )));
            }
            Ok(())
        })
    }
}

pub fn current_id(py: Python<'_>) -> PyResult<Option<i64>> {
    let Some(variables) = context_variables(py)? else {
        return Ok(None);
    };
    variables.batch_id.bind(py).call_method0("get")?.extract()
}

pub fn current(py: Python<'_>, ctx: &Arc<AppContext>) -> PyResult<Option<Arc<TransactionState>>> {
    let Some(variables) = context_variables(py)? else {
        return Ok(None);
    };
    let value = variables.transaction.bind(py).call_method0("get")?;
    if value.is_none() {
        return Ok(None);
    }
    let transaction = value.cast::<BatchTransaction>()?;
    let state = transaction.borrow().state.clone();
    Ok(Arc::ptr_eq(&state.ctx, ctx).then_some(state))
}

pub fn with_current<T>(
    py: Python<'_>,
    state: Arc<TransactionState>,
    f: impl FnOnce() -> PyResult<T>,
) -> PyResult<T> {
    let variable = context_variables(py)?
        .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Batch context is unavailable"))?
        .transaction
        .bind(py);
    let token = variable.call_method1("set", (Py::new(py, BatchTransaction { state })?,))?;
    let result = f();
    variable.call_method1("reset", (token,))?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::{
        ConnectOptions, ConnectionTrait, Database, DbBackend, Statement, TransactionTrait,
    };

    #[tokio::test]
    async fn outstanding_enqueue_closes_transaction_and_eventually_rolls_back() {
        Python::initialize();
        let db = Arc::new(Database::connect("sqlite::memory:").await.unwrap());
        let ctx = Arc::new(AppContext::new(
            crate::database_url::DatabaseUrl::parse("sqlite::memory:").unwrap(),
            Some(db.clone()),
            ConnectOptions::new("sqlite::memory:"),
            None,
        ));
        let state = TransactionState::new(ctx, db.begin().await.unwrap());
        let held = state.connection().unwrap();
        held.execute(Statement::from_string(
            DbBackend::Sqlite,
            "CREATE TABLE rollback_probe (id INTEGER)",
        ))
        .await
        .unwrap();

        let error = state.clone().finish(true).await.unwrap_err().to_string();
        assert!(error.contains("closed without commit"));
        assert!(error.contains("cannot be reused"));
        assert!(state.connection().is_err());
        assert!(state.finish(true).await.is_err());
        drop(held);

        let row = db
            .query_one(Statement::from_string(
                DbBackend::Sqlite,
                "SELECT COUNT(*) AS count FROM sqlite_master WHERE name = 'rollback_probe'",
            ))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.try_get::<i64>("", "count").unwrap(), 0);
    }
}
