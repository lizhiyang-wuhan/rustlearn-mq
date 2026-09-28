//! Demo 3：线程间通信 —— `crossbeam` 进阶（作用域线程 / mpmc / select / 背压）。
//!
//! 运行：`cargo run --bin thread_crossbeam`
//!
//! std 的 `mpsc` 已能覆盖很多场景，但 `crossbeam-channel` 提供了几项生产级增强：
//! - **mpmc**：接收端也可克隆，支持多消费者，天然适合「工作池 / 流水线」。
//! - **有界通道 + 背压**：`bounded(n)` 在满时会阻塞发送方，防止内存被撑爆。
//! - **`select!`**：在多个通道上同时等待，谁先来就处理谁（还能带超时）。
//! - **作用域线程 `scope`**：线程可以直接借用栈上的局部变量，无需 `Arc`/`'static`，
//!   且 scope 结束时保证所有子线程已 join，杜绝悬垂引用。

use crossbeam::channel::{bounded, select, unbounded};
use crossbeam::thread::scope;
use std::time::{Duration, Instant};

fn main() {
    demo_scoped_threads();
    demo_mpmc_pipeline();
    demo_select();
}

/// 1) 作用域线程：直接借用局部数据，scope 结束时自动 join 全部线程。
fn demo_scoped_threads() {
    println!("== 作用域线程 scope：借用局部变量，无需 Arc ==");
    // data 是栈上的局部变量，普通 thread::spawn 无法借用它（要求 'static），
    // 但 scope 内的线程可以直接借用，且 scope 保证退出前所有线程都已结束。
    let data = vec![1, 2, 3, 4, 5, 6, 7, 8];
    let thread_n = 4;
    let chunk = data.len().div_ceil(thread_n);

    // 用一个通道收集每个线程的部分和（scope + channel 是黄金搭档）。
    let (tx, rx) = unbounded::<i32>();
    scope(|s| {
        for part in data.chunks(chunk) {
            // part 直接借用了栈上的 data —— 无需 Arc、无需 'static。
            let tx = tx.clone();
            s.spawn(move |_| {
                let sum: i32 = part.iter().sum();
                tx.send(sum).unwrap();
            });
        }
        drop(tx); // drop 原始发送端，rx 迭代才会在收齐后结束。
        let total: i32 = rx.iter().sum();
        // 这里在 scope 主线程内直接读取 total，无需再跨线程传递。
        println!("  各分片部分和汇总 = {total}（预期 {}）\n", data.iter().sum::<i32>());
    })
    .unwrap();
}

/// 2) mpmc 流水线：多个生产者 -> 有界通道 -> 多个消费者（工作池）。
fn demo_mpmc_pipeline() {
    println!("== mpmc 工作池：3 生产者 -> 有界通道 -> 2 消费者 ==");
    // 有界通道：容量 4。发送方在通道满时会阻塞，这就是「背压」。
    let (tx, rx) = bounded::<i64>(4);

    scope(|s| {
        // 生产者：克隆 tx。
        for p in 0..3 {
            let tx = tx.clone();
            s.spawn(move |_| {
                for i in 1..=4 {
                    let v = (p as i64 + 1) * i;
                    tx.send(v).unwrap();
                }
            });
        }
        drop(tx); // 关键：drop 原始 tx，所有克隆结束后 rx 迭代才会结束。

        // 消费者：克隆 rx（mpmc 的接收端可克隆）。
        for c in 0..2 {
            let rx = rx.clone();
            s.spawn(move |_| {
                let (mut total, mut count) = (0, 0);
                // for 迭代会阻塞取消息，直到所有 tx 被 drop 且通道排空。
                for v in rx {
                    total += v;
                    count += 1;
                    // 模拟处理耗时：让两个消费者有机会公平竞争消息，而不是一个抢光。
                    std::thread::sleep(Duration::from_millis(2));
                }
                println!("  consumer-{c} 处理 {count} 条，累加和 = {total}");
            });
        }
        // rx 原始句柄也要 drop，否则消费者线程的 for 循环不会结束。
        drop(rx);
    })
    .unwrap();
    println!();
}

/// 3) select!：同时等待多个通道，带超时。
fn demo_select() {
    println!("== select!：多路复用 + 超时 ==");
    let (fast_tx, fast_rx) = unbounded();
    let (slow_tx, slow_rx) = unbounded();

    scope(|s| {
        // 向线程传入发送端的克隆，主线程保留原始句柄，
        // 这样在整个 select 循环期间通道不会“断开”（断开会被 select 当作就绪）。
        let fast_tx2 = fast_tx.clone();
        s.spawn(move |_| {
            std::thread::sleep(Duration::from_millis(20));
            fast_tx2.send("快通道消息").unwrap();
        });
        let slow_tx2 = slow_tx.clone();
        s.spawn(move |_| {
            std::thread::sleep(Duration::from_millis(200));
            slow_tx2.send("慢通道消息").unwrap();
        });

        let start = Instant::now();
        // 循环直到两条消息都收到；期间会自然地命中一次“超时”分支。
        let mut got = 0;
        while got < 2 {
            select! {
                recv(fast_rx) -> m => {
                    println!("  [{}ms] 命中快通道: {:?}", start.elapsed().as_millis(), m.unwrap());
                    got += 1;
                }
                recv(slow_rx) -> m => {
                    println!("  [{}ms] 命中慢通道: {:?}", start.elapsed().as_millis(), m.unwrap());
                    got += 1;
                }
                // default(超时)：在指定时间内没有任何通道就绪时执行。
                default(Duration::from_millis(80)) => {
                    println!("  [{}ms] 超时：暂无就绪消息，继续等待…", start.elapsed().as_millis());
                }
            }
        }
        // 循环结束后显式 drop 原始发送端，让子线程能正常结束。
        drop(fast_tx);
        drop(slow_tx);
    })
    .unwrap();
}
