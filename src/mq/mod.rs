//! 迷你 Kafka 式消息队列（单机、纯 Rust、仅用 std + serde 实现）。
//!
//! 核心思想借鉴 Kafka：**每个 topic 是一条 append-only（只追加）的日志**，
//! 每条消息在日志中有一个单调递增的 `offset`。生产者把消息追加到日志末尾；
//! 消费者可以从任意 offset 开始「重放」历史消息，重放完再「实时跟随」新消息。
//! 这正是 Kafka 区别于传统消息队列（消息被消费后即删除）的关键：日志可重复消费。
//!
//! 模块划分：
//! - [`protocol`]：客户端与 broker 之间的线协议（换行分隔的 JSON）。
//! - [`broker`]  ：服务端，维护 topic 日志并向订阅者扇出消息。
//! - [`client`]  ：`Producer` / `Consumer` 客户端封装。

pub mod broker;
pub mod client;
pub mod protocol;

pub use broker::{BrokerHandle, spawn_broker};
pub use client::{Consumer, Producer};
