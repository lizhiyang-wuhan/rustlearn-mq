//! Demo 1：线程间通信 —— std 的 `mpsc`（多生产者 / 单消费者）通道。
//!
//! 运行：`cargo run --bin thread_mpsc`
//!
//! 核心最佳实践：**「不要通过共享内存来通信，而要通过通信来共享内存」**。
//! 通道（channel）把数据的所有权从一个线程「移动」到另一个线程，
//! 编译器借此在编译期就杜绝了数据竞争，比裸 `Arc<Mutex<_>>` 更安全、更清晰。
//!
//! `mpsc` 允许多个发送端（`Sender` 可 `clone`）但只有一个接收端（`Receiver` 不可克隆）。

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

fn main() {
    // 创建通道：tx 是发送端，rx 是接收端。
    let (tx, rx) = mpsc::channel::<String>();

    let worker_count = 4;
    for id in 0..worker_count {
        // 每个 worker 克隆一份发送端 —— 这就是「多生产者」。
        let tx = tx.clone();
        thread::spawn(move || {
            for task in 0..3 {
                // 模拟耗时工作，然后把结果「发送」回主线程。
                thread::sleep(Duration::from_millis((id * 10 + task * 5) as u64));
                // send 会把值的所有权转移进通道；发送失败意味着接收端已被 drop。
                tx.send(format!("worker-{id} 完成任务 #{task}"))
                    .expect("接收端应仍存活");
            }
            // worker 结束时其 tx 副本被 drop。当所有 tx 都被 drop 后，
            // rx 的迭代会自然结束 —— 这是优雅「结束信号」的关键。
        });
    }

    // 主线程持有原始的 tx。必须在开始接收前 drop 掉，
    // 否则 rx 永远认为「还有发送端存活」，下面的 for 循环不会退出。
    drop(tx);

    println!("主线程开始接收各 worker 的结果：");
    // rx 本身是迭代器：会阻塞等待消息，直到所有发送端 drop 且通道排空。
    let mut received = 0;
    for msg in rx {
        received += 1;
        println!("  收到: {msg}");
    }

    println!("\n共收到 {received} 条消息（预期 {} 条）。", worker_count * 3);
    println!("要点：rx 的循环能自动结束，是因为所有 tx 都已被 drop。");
}
