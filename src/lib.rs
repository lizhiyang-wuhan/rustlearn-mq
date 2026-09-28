//! kafkaexample —— 单机消息队列 / 线程间通信 / 进程间通信 的 Rust 最佳实践教学仓库。
//!
//! 本 crate 由一个库（`mq` 模块：迷你 Kafka 式消息队列）和若干可执行 demo（`src/bin/*`）组成。
//! 直接运行 `cargo run --bin <名称>` 即可查看每个 demo 的输出，源码里有大量中文注释讲解要点。
//!
//! 各 demo 一览：
//! - `thread_mpsc`         : std 多生产者单消费者通道（消息传递）
//! - `thread_shared_state` : Arc<Mutex> / Arc<RwLock> 共享状态
//! - `thread_crossbeam`    : crossbeam 作用域线程 + mpmc + select + 背压
//! - `ipc_unix_socket`     : Unix Domain Socket 进程间双向通信
//! - `ipc_child_pipe`      : 父子进程 stdin/stdout 管道通信
//! - `mq_broker`           : 独立运行的消息队列 broker（类似 kafka-server-start）
//! - `mq_demo`             : 进程内启动 broker + producer + consumer 的端到端演示

pub mod mq;
