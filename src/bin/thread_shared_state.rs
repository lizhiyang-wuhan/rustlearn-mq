//! Demo 2：线程间通信 —— 共享状态（`Arc` + `Mutex` / `RwLock` / 原子类型）。
//!
//! 运行：`cargo run --bin thread_shared_state`
//!
//! 当多个线程需要读写同一份数据时，用 `Arc<T>` 共享所有权，再用内部可变性保护并发访问：
//! - `Arc<AtomicU64>`：简单计数器的**首选**，无锁、最快。
//! - `Arc<Mutex<T>>`  ：读写都需要独占，适合写多或读写相当的场景。
//! - `Arc<RwLock<T>>` ：允许多读单写，适合「读远多于写」的场景。
//!
//! 最佳实践：
//! 1. **临界区尽量短**：只在锁内做必要的读写，别在持锁时做 IO / sleep / 复杂计算。
//! 2. **绝不在持锁时再申请另一把锁**（除非全局顺序固定），否则易死锁。
//! 3. 锁守卫（guard）离开作用域自动解锁 —— 优先用 `{ ... }` 限定其生命周期。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;

fn main() {
    demo_atomic();
    demo_mutex();
    demo_rwlock();
}

/// 1) 原子类型：无锁计数，性能最好。
fn demo_atomic() {
    println!("== Arc<AtomicU64>：无锁计数 ==");
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for _ in 0..8 {
        let counter = Arc::clone(&counter);
        handles.push(thread::spawn(move || {
            for _ in 0..10_000 {
                // fetch_add 是原子操作，多线程并发累加不会丢失更新。
                counter.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    println!("  结果 = {}（预期 80000）\n", counter.load(Ordering::SeqCst));
}

/// 2) Mutex：保护一段需要「读-改-写」的复合操作。
fn demo_mutex() {
    println!("== Arc<Mutex<Vec<i32>>>：互斥保护复合操作 ==");
    let data = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();

    for id in 0..4 {
        let data = Arc::clone(&data);
        handles.push(thread::spawn(move || {
            for i in 0..5 {
                // lock() 返回 guard；这里用一个小作用域限定它，尽快释放锁。
                let mut guard = data.lock().unwrap();
                guard.push(id * 100 + i);
            } // guard 在此 drop，锁被释放。
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let guard = data.lock().unwrap();
    println!("  收集到 {} 个元素，和为 {}\n", guard.len(), guard.iter().sum::<i32>());
}

/// 3) RwLock：读多写少时，多个读者可并发持有读锁。
fn demo_rwlock() {
    println!("== Arc<RwLock<String>>：读写锁 ==");
    let config = Arc::new(RwLock::new(String::from("初始配置")));
    let mut handles = Vec::new();

    // 一个写线程。
    {
        let config = Arc::clone(&config);
        handles.push(thread::spawn(move || {
            // write() 独占：期间没有任何读者能进入。
            *config.write().unwrap() = String::from("已更新的配置");
        }));
    }
    // 多个读线程可并发。
    for id in 0..3 {
        let config = Arc::clone(&config);
        handles.push(thread::spawn(move || {
            // read() 共享：多个读者可同时持有读锁。
            let snapshot = config.read().unwrap().clone();
            println!("  reader-{id} 读到: {snapshot}");
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    println!("  最终配置: {}\n", config.read().unwrap());
}
