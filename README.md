# Quebec

Quebec 是一个简单的后台任务队列，用于处理异步任务。其名称来源于 "Queue" 的首字母 "Q"，"Q" 也可以认为是 "Queue" 的谐音，在北约音标中读作 "Quebec"
该项目受到 [Solid Queue](https://github.com/rails/solid_queue) 的启发。

为什么要写一个这样的项目：

- 为了简化架构，不依赖 Redis 或者 MQ 等其他服务。
- 可以充分利用 RDBMS 的复杂查询功能，比如可以方便查询某些任务的状态。如果你用过 [Sidekiq](https://github.com/sidekiq/sidekiq) 查询大量执行失败的任务，你就会明白这个优势。

为什么使用 Rust：

- 我在学习 Rust，所以想要练习一下。
- Rust 的性能和安全性。
- 这样不依赖任何其他 Python 的库，可以避免很多奇怪的问题。
- asyncio, Trio, threading, SQLAlchmey, Django, FastAPI 可以任意搭配使用。

特性：

- scheduled tasks
- recurring tasks
- concurrency control
- dashboard
- retry

数据库支持：

- SQLite
- PostgreSQL
- MySQL

SolidQueue 兼容
