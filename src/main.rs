//! kafkaexample —— 单机「消息队列 / 线程间通信 / 进程间通信」的 Rust 最佳实践合集。
//!
//! 直接 `cargo run` 会打印本索引；每个 demo 用 `cargo run --bin <名称>` 单独运行。

fn main() {
    let demos = [
        ("thread_mpsc", "线程间通信：std mpsc 多生产者单消费者通道（消息传递优于共享内存）"),
        ("thread_shared_state", "线程间通信：Arc + Atomic/Mutex/RwLock 共享状态的正确用法"),
        ("thread_crossbeam", "线程间通信：crossbeam 作用域线程 + mpmc 工作池 + select + 背压"),
        ("ipc_unix_socket", "进程间通信：Unix Domain Socket（父进程拉起服务端子进程并双向通信）"),
        ("ipc_child_pipe", "进程间通信：父子进程 stdin/stdout 管道（shell `a | b` 的原理）"),
        ("mq_broker", "消息队列：独立运行的 mini-Kafka broker（监听 127.0.0.1:9092）"),
        ("mq_demo", "消息队列：端到端演示（发布/订阅 + offset + 重放 + 实时跟随）"),
    ];

    println!("kafkaexample —— 单机消息队列 / 线程间通信 / 进程间通信 教学 demo\n");
    println!("运行任意一个 demo：\n");
    for (bin, desc) in demos {
        println!("  cargo run --bin {bin}");
        println!("      {desc}\n");
    }
    println!("建议学习顺序：thread_* -> ipc_* -> mq_*（mq_demo 会串起前面所有概念）。");
}
