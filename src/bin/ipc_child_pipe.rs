//! Demo 5：进程间通信 —— 父子进程标准输入/输出管道（pipe）。
//!
//! 运行：`cargo run --bin ipc_child_pipe`
//!
//! 管道是最古老、最简单的 IPC：父进程用 `Stdio::piped()` 启动子进程，
//! 拿到子进程的 `stdin`（父写子读）与 `stdout`（子写父读），形成两条单向字节流。
//! 这正是 shell 里 `a | b` 的底层机制。
//!
//! 本 demo 同样自包含：把自己以 `--child` 参数再启动一份作为子进程，
//! 子进程把每行输入转成「带序号的大写」回写，父进程读取展示。
//!
//! 最佳实践：
//! - 写完 stdin 后要 `drop` 或 `shutdown`，让子进程读到 EOF 从而结束循环，否则可能死锁。
//! - 避免「父写满管道缓冲、子也在等父读」造成的相互死锁；数据量大时应边写边读或用线程分离读写。

use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--child") {
        return run_child();
    }
    run_parent()
}

/// 父进程：启动子进程，向其 stdin 写入若干行，再从 stdout 读取处理结果。
fn run_parent() -> std::io::Result<()> {
    let exe = std::env::current_exe()?;
    println!("[parent] 启动子进程: {} --child", exe.display());

    let mut child = Command::new(exe)
        .arg("--child")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    // 取出 stdin / stdout 句柄（用 take 避免后续 wait 时的借用冲突）。
    let mut child_stdin = child.stdin.take().expect("stdin 已配置为 piped");
    let child_stdout = child.stdout.take().expect("stdout 已配置为 piped");

    // 用独立线程读取子进程输出，防止「父写 -> 子输出缓冲满 -> 双方互等」的死锁。
    let reader_thread = std::thread::spawn(move || {
        let mut reader = BufReader::new(child_stdout);
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                break; // 子进程关闭了 stdout。
            }
            println!("[parent] 收到子进程输出: {}", line.trim_end());
        }
        std::io::Result::Ok(())
    });

    // 向子进程写入几行数据。
    for i in 1..=4 {
        let msg = format!("message-{i}");
        println!("[parent] 发送: {msg}");
        writeln!(child_stdin, "{msg}")?;
    }
    child_stdin.flush()?;
    // 关键：关闭 stdin，让子进程读到 EOF 后退出。
    drop(child_stdin);

    reader_thread.join().unwrap()?;
    let status = child.wait()?;
    println!("[parent] 子进程退出: {status}");
    Ok(())
}

/// 子进程：从 stdin 逐行读取，转换后写到 stdout。
fn run_child() -> std::io::Result<()> {
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut out = stdout.lock();

    let mut line = String::new();
    let mut seq = 0;
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break; // 父进程关闭了 stdin。
        }
        seq += 1;
        writeln!(out, "#{seq} {}", line.trim_end().to_uppercase())?;
        out.flush()?;
    }
    // 用 stderr（父进程 inherit）输出提示，避免混入 stdout 的管道数据流。
    eprintln!("[child] stdin 已关闭，子进程结束（共处理 {seq} 行）");
    Ok(())
}
