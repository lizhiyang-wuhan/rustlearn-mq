//! Demo 6：独立运行的消息队列 broker（类似 `kafka-server-start`）。
//!
//! 运行：`cargo run --bin mq_broker`
//! 默认监听 `127.0.0.1:9092`（可用第一个参数覆盖，如 `cargo run --bin mq_broker -- 127.0.0.1:19092`）。
//!
//! 启动后它会一直运行，等待 producer / consumer 连接（按 Ctrl-C 结束）。
//! 你可以另开一个终端用 `mq_demo` 或自己写的客户端连上来。
//! 这里用 KRaft 风格的「单进程 broker」，不依赖 ZooKeeper，专注演示消息队列本身。

use kafkaexample::mq::spawn_broker;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9092".to_string());

    let handle = spawn_broker(&addr)?;
    println!("消息队列 broker 已启动，监听 {}", handle.addr);
    println!("提示：producer/consumer 可连接到该地址；按 Ctrl-C 停止。");

    // accept 循环在后台线程运行，主线程在此驻留即可。
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
