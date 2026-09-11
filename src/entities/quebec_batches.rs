//! `SeaORM` Entity for Solid Queue batches (`solid_queue_batches`).

#[cfg(feature = "python")]
use pyo3::prelude::*;
use sea_orm::entity::prelude::*;

#[cfg_attr(feature = "python", pyclass(name = "QuebecBatch", from_py_object))]
#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[sea_orm(table_name = "solid_queue_batches")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub active_job_batch_id: Option<String>,
    pub description: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub on_finish: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub on_success: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub on_failure: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub metadata: Option<String>,
    pub total_jobs: i32,
    pub completed_jobs: i32,
    pub failed_jobs: i32,
    pub enqueued_at: Option<DateTime>,
    pub finished_at: Option<DateTime>,
    pub failed_at: Option<DateTime>,
    pub created_at: DateTime,
    pub updated_at: DateTime,
}

#[cfg(feature = "python")]
#[pymethods]
impl Model {
    #[getter]
    pub fn get_id(&self) -> i64 {
        self.id
    }

    #[getter]
    pub fn get_active_job_batch_id(&self) -> Option<String> {
        self.active_job_batch_id.clone()
    }

    #[getter]
    pub fn get_description(&self) -> Option<String> {
        self.description.clone()
    }

    #[getter]
    pub fn get_on_finish(&self) -> Option<String> {
        self.on_finish.clone()
    }

    #[getter]
    pub fn get_on_success(&self) -> Option<String> {
        self.on_success.clone()
    }

    #[getter]
    pub fn get_on_failure(&self) -> Option<String> {
        self.on_failure.clone()
    }

    #[getter]
    pub fn get_metadata(&self) -> Option<String> {
        self.metadata.clone()
    }

    #[getter]
    pub fn get_total_jobs(&self) -> i32 {
        self.total_jobs
    }

    #[getter]
    pub fn get_completed_jobs(&self) -> i32 {
        self.completed_jobs
    }

    #[getter]
    pub fn get_failed_jobs(&self) -> i32 {
        self.failed_jobs
    }

    #[getter]
    pub fn get_enqueued_at(&self) -> Option<DateTime> {
        self.enqueued_at
    }

    #[getter]
    pub fn get_finished_at(&self) -> Option<DateTime> {
        self.finished_at
    }

    #[getter]
    pub fn get_failed_at(&self) -> Option<DateTime> {
        self.failed_at
    }

    #[getter]
    pub fn get_created_at(&self) -> DateTime {
        self.created_at
    }

    #[getter]
    pub fn get_updated_at(&self) -> DateTime {
        self.updated_at
    }
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
