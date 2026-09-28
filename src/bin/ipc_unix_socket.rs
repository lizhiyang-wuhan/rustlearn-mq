//! Demo 4：进程间通信 —— Unix Domain Socket（UDS）。
//!
//! 运行：`cargo run --bin ipc_unix_socket`
//!
//! UDS 是**单机**进程间通信的首选：走内核、不经过网络协议栈，比 TCP 回环更快，
//! 还能借助文件系统权限做访问控制（Docker、PostgreSQL、systemd 都用它）。
//!
//! 本 demo 是「真·跨进程」：父进程把**自己这个可执行文件**以 `--server` 参数再启动一份
//! 作为服务端子进程，然后作为客户端通过 socket 文件与之通信。
//!
//! 最佳实践：
//! - socket 路径放在临时目录且带唯一后缀，避免多实例冲突。
//! - bind 前先删除同名「陈旧 socket 文件」（上次异常退出可能残留）。
//! - 用完删除 socket 文件；客户端需等待服务端真正 bind 完成后再连接。

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // 子进程模式：作为 UDS 服务端运行。
    if args.len() >= 3 && args[1] == "--server" {
        
        return run_server(&args[2]);
    }

    // 父进程模式：作为客户端，先拉起服务端子进程。
    let sock_path = std::env::temp_dir().join(format!("kafkaexample-uds-{}.sock", std::process::id()));
    let sock_str = sock_path.to_string_lossy().to_string();

    // bind 前清理可能残留的 socket 文件。
    let _ = std::fs::remove_file(&sock_path);

    let exe = std::env::current_exe()?;
    println!("[client] 启动服务端子进程: {} --server {}", exe.display(), sock_str);
    let mut child = Command::new(exe)
        .args(["--server", &sock_str])
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;

    // 等待服务端把 socket 文件创建出来（轮询 + 超时，避免竞态）。
    let deadline = Instant::now() + Duration::from_secs(5);
    while !Path::new(&sock_str).exists() {
        if Instant::now() > deadline {
            let _ = child.kill();
            return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "等待服务端超时"));
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    // 连接并做几轮请求-响应。
    let mut stream = UnixStream::connect(&sock_str)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    println!("[client] 已连接到 {sock_str}");

    for msg in ["hello", "进程间通信", "rust best practice"] {
        writeln!(stream, "{msg}")?;
        stream.flush()?;
        let mut resp = String::new();
        reader.read_line(&mut resp)?;
        print!("[client] 发送 {msg:?} -> 服务端回复 {:?}", resp.trim_end());
        println!();
    }

    // 发送结束标记，服务端读到 EOF/quit 后退出。
    writeln!(stream, "quit")?;
    stream.flush()?;
    drop(stream); // 关闭连接，服务端 accept 循环随客户端断开而结束。

    let status = child.wait()?;
    let _ = std::fs::remove_file(&sock_path); // 清理 socket 文件。
    println!("[client] 服务端已退出: {status}");
    Ok(())
}

/// UDS 服务端：绑定 socket 文件，接受一个连接，逐行处理直到客户端断开或发送 quit。
fn run_server(sock_path: &str) -> std::io::Result<()> {
    // 再次确保没有陈旧文件（防御性）。
    let _ = std::fs::remove_file(sock_path);
    let listener = UnixListener::bind(sock_path)?;
    println!("[server] 监听于 {sock_path}");

    for incoming in listener.incoming() {
        let stream = incoming?;
        let mut reader = BufReader::new(stream.try_clone()?);
        let mut writer = stream;
        println!("[server] 客户端已连接");

        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line)?;
            if n == 0 {
                // EOF：客户端关闭了连接。
                break;
            }
            let text = line.trim_end();
            if text == "quit" {
                let _ = writeln!(writer, "bye");
                println!("[server] 收到 quit，结束");
                let _ = std::fs::remove_file(sock_path);
                return Ok(());
            }
            // 业务逻辑：返回大写 + 字节长度，演示双向通信。
            let resp = format!("{} (len={})", text.to_uppercase(), text.len());
            writeln!(writer, "{resp}")?;
            writer.flush()?;
        }
        println!("[server] 连接关闭");
    }
    Ok(())
}
