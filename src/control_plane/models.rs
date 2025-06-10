use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub jobs_count: i64,
    pub status: String,
}

#[derive(Debug, Serialize)]
pub struct WorkerInfo {
    pub id: i64,
    pub name: String,
    pub kind: String,
    pub hostname: String,
    pub pid: i32,
    pub last_heartbeat_at: String,
    pub seconds_since_heartbeat: i64,
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct Pagination {
    #[serde(default = "default_page")]
    pub page: u64,
}

fn default_page() -> u64 {
    1
}

#[derive(Debug, Serialize)]
pub struct FailedJobInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub error: String,
    pub failed_at: String,
}

#[derive(Debug, Serialize)]
pub struct InProgressJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub worker_id: i64,
    pub started_at: String,
    pub runtime: String,
}

#[derive(Debug, Serialize)]
pub struct QueueJobInfo {
    pub id: i64,
    pub class_name: String,
    pub status: String,
    pub created_at: String,
    pub execution_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct ScheduledJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub created_at: String,
    pub scheduled_at: String,
    pub scheduled_in: String,
}

#[derive(Debug, Serialize)]
pub struct BlockedJobInfo {
    pub id: i64,
    pub job_id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub concurrency_key: String,
    pub created_at: String,
    pub waiting_time: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct FinishedJobInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub created_at: String,
    pub finished_at: String,
    pub runtime: String,
}

#[derive(Debug, Serialize)]
pub struct JobDetailsInfo {
    pub id: i64,
    pub queue_name: String,
    pub class_name: String,
    pub status: String,
    pub created_at: String,
    pub failed_at: Option<String>,
    pub scheduled_at: Option<String>,
    pub scheduled_in: Option<String>,
    pub concurrency_key: Option<String>,
    pub waiting_time: Option<String>,
    pub expires_at: Option<String>,
    pub started_at: Option<String>,
    pub runtime: Option<String>,
    pub worker_id: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub backtrace: Option<String>,
    pub arguments: String,
    pub context: Option<String>,
    pub execution_id: Option<i64>,
    pub execution_history: Vec<ExecutionHistoryItem>,
}

#[derive(Debug, Serialize)]
pub struct ExecutionHistoryItem {
    pub attempt: i32,
    pub timestamp: String,
    pub status: String,
    pub error: Option<String>,
}