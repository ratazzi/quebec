# Quebec

Quebec is a simple background task queue for processing asynchronous tasks. The name is derived from the NATO phonetic alphabet for "Q", representing "Queue".

This project is inspired by [Solid Queue](https://github.com/rails/solid_queue).

> **Warning:** This project is in early development stage. Not recommended for production use.

## Why Quebec?

- **Simplified Architecture**: No dependencies on Redis or message queues
- **Database-Powered**: Leverages RDBMS capabilities for complex task queries and management
- **Rust Implementation**: High performance and safety with Python compatibility
- **Framework Agnostic**: Works with asyncio, Trio, threading, SQLAlchemy, Django, FastAPI, etc.

## Features

- Scheduled tasks
- Recurring tasks
- Concurrency control
- Web dashboard
- Automatic retries
- Signal handling
- Lifecycle hooks

## Database Support

- SQLite
- PostgreSQL
- MySQL

## Quick Start

```python
import sys
import time
import queue
import logging
import threading

# Setup logging
FORMAT = '%(asctime)s %(levelname)s [%(name)s:%(filename)s:%(lineno)d]: %(message)s'
logging.basicConfig(stream=sys.stdout, format=FORMAT, level=logging.DEBUG)
logger = logging.getLogger()

import quebec

# Initialize Quebec with your database connection
dsn = 'sqlite://myapp.db?mode=rwc'
qc = quebec.Quebec(dsn, use_skip_locked=True)

# Create tables if they don't exist
qc.create_table()

# Setup signal handling for graceful shutdown
qc.setup_signal_handler()
event = threading.Event()

# Register lifecycle hooks
@qc.on_start
def on_start():
    logger.info("Quebec is starting...")

@qc.on_stop
def on_stop():
    logger.info("Quebec is stopping...")

@qc.on_worker_start
def on_worker_start():
    logger.info("Worker is starting...")

@qc.on_worker_stop
def on_worker_stop():
    logger.info("Worker is stopping...")

@qc.on_shutdown
def cleanup():
    logger.info("Shutting down gracefully...")
    event.set()

# Define a job
@qc.register_job
class MyJob(quebec.BaseClass):
    def perform(self, *args, **kwargs):
        self.logger.info(f"Processing job {self.id} with args: {args}, kwargs: {kwargs}")
        # Your job logic here

# Queue a job for immediate execution
MyJob.perform_later(qc, "task_data", param1="value1")

# Start the dispatcher, scheduler, and worker polling in non-blocking mode
qc.spawn_dispatcher()
qc.spawn_scheduler()
qc.spawn_job_claim_poller()

# Setup worker threads to process jobs
q = queue.Queue()
qc.feed_jobs_to_queue(q)
threading.Thread(target=quebec.ThreadedRunner(q, event).run).start()

# Main application loop
while True:
    try:
        time.sleep(1)
    except KeyboardInterrupt:
        logger.debug('KeyboardInterrupt received, shutting down...')
        qc.graceful_shutdown()
```

## Lifecycle Hooks

Quebec provides several lifecycle hooks that you can use to execute code at different stages of the application lifecycle:

- `@qc.on_start`: Called when Quebec starts
- `@qc.on_stop`: Called when Quebec stops
- `@qc.on_worker_start`: Called when a worker starts
- `@qc.on_worker_stop`: Called when a worker stops
- `@qc.on_shutdown`: Called during graceful shutdown

These hooks are useful for:
- Initializing resources
- Cleaning up resources
- Logging application state
- Monitoring worker lifecycle
- Graceful shutdown handling
