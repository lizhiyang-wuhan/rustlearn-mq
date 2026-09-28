//! Demo 7：消息队列端到端演示（进程内启动 broker + producer + 2 个 consumer）。
//!
//! 运行：`cargo run --bin mq_demo`
//!
//! 这个 demo 一次性展示消息队列最核心的几个概念：
//! 1. **发布/订阅**：producer 向 topic 发消息，consumer 订阅 topic 收消息，双方解耦。
//! 2. **offset 与日志**：每条消息在 topic 日志里有稳定递增的 offset。
//! 3. **重放（replay）**：从 offset=0 订阅的消费者会先收到全部历史消息 —— 这是 Kafka
//!    相对传统 MQ 的关键优势（消息不因被消费而消失，可回溯、可重复消费）。
//! 4. **实时跟随**：从「当前末尾 offset」订阅的消费者只收到之后新发布的消息。
//!
//! 时间线：
//!   producer 先发 3 条 -> replay 消费者(从头)与 live 消费者(从末尾)分别订阅
//!   -> producer 再发 2 条 -> 两个消费者都实时收到这 2 条。

use kafkaexample::mq::{Consumer, Producer, spawn_broker};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {
    // 用端口 0 让内核分配空闲端口，避免与本机其他服务冲突。
    let broker = spawn_broker("127.0.0.1:0")?;
    let addr = broker.addr.to_string();
    let topic = "orders";
    println!("=== 消息队列端到端演示（broker @ {addr}）===\n");

    // 1) 生产者先发布 3 条历史消息。
    let mut producer = Producer::connect(&addr)?;
    for i in 1..=3 {
        let payload = format!("历史订单-{i}");
        let offset = producer.publish(topic, &payload)?;
        println!("[producer] 发布 {payload:?} -> offset={offset}");
    }
    let end = producer.end_offset(topic)?;
    println!("[producer] 当前日志末尾 offset = {end}\n");

    // 2) replay 消费者：从 offset 0 订阅，应收到 3 条历史 + 2 条新消息 = 5 条。
    let replay_addr = addr.clone();
    let replay_consumer = thread::spawn(move || -> std::io::Result<()> {
        let mut c = Consumer::subscribe(&replay_addr, topic, 0)?;
        println!("[replay-consumer] 从头订阅，期待收到 5 条：");
        for _ in 0..5 {
            if let Some(m) = c.recv()? {
                println!("  [replay-consumer] offset={} payload={:?}", m.offset, m.payload);
            }
        }
        Ok(())
    });

    // 3) live 消费者：从当前末尾 offset 订阅，只收之后的新消息（2 条）。
    let live_addr = addr.clone();
    let live_consumer = thread::spawn(move || -> std::io::Result<()> {
        let mut c = Consumer::subscribe(&live_addr, topic, end)?;
        println!("[live-consumer] 从 offset={end} 订阅，只收新消息：");
        for _ in 0..2 {
            if let Some(m) = c.recv()? {
                println!("  [live-consumer] offset={} payload={:?}", m.offset, m.payload);
            }
        }
        Ok(())
    });

    // 等待两个消费者完成订阅注册（真实项目里应通过握手确认，这里为演示简化）。
    thread::sleep(Duration::from_millis(200));

    // 4) 生产者再发 2 条新消息，两个消费者都应实时收到。
    println!("\n[producer] 再发布 2 条新消息：");
    for i in 4..=5 {
        let payload = format!("实时订单-{i}");
        let offset = producer.publish(topic, &payload)?;
        println!("[producer] 发布 {payload:?} -> offset={offset}");
    }

    replay_consumer.join().unwrap()?;
    live_consumer.join().unwrap()?;

    println!("\n=== 演示结束，关闭 broker ===");
    broker.shutdown();
    Ok(())
}
