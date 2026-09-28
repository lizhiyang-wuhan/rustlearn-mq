//! 消息队列 broker（服务端）。
//!
//! 并发模型（这是本 demo 的关键最佳实践）：
//! - 每个客户端连接由一个独立线程处理（thread-per-connection），简单直观。
//! - 全局状态 `HashMap<topic, TopicLog>` 用一把 `Mutex` 保护，**只在极短的临界区内访问**。
//! - 订阅者各自持有一个 `crossbeam` 有界通道的 `Sender`。发布消息时：
//!   在锁内「追加日志 + 克隆出所有订阅者的 Sender」，**出锁后再发送**，
//!   避免因为某个慢消费者阻塞而长时间持有全局锁。
//! - 订阅时：在**同一次加锁**内「快照历史日志 + 注册订阅者」，
//!   保证重放与实时推送之间既无空隙也无重复。

use std::collections::HashMap;
use std::io::BufReader;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, bounded};

use super::protocol::{Request, Response, read_request, write_response};

/// 单个订阅者通道的缓冲上限：给慢消费者提供「背压」，同时避免瞬时突发丢消息。
const SUBSCRIBER_CHANNEL_CAPACITY: usize = 1024;

/// 一个 topic 的只追加日志 + 活跃订阅者集合。
#[derive(Default)]
struct TopicLog {
    /// 消息日志，下标即 offset。
    messages: Vec<String>,
    /// 活跃订阅者：`(订阅者 id, 发送端)`。
    subscribers: Vec<(u64, Sender<Response>)>,
}

/// 全局共享状态。
#[derive(Default)]
struct State {
    topics: HashMap<String, TopicLog>,
}

/// broker 运行句柄，可用于获取监听地址与优雅关闭。
pub struct BrokerHandle {
    /// 实际监听地址（用端口 0 启动时由内核分配，可从这里读到）。
    pub addr: SocketAddr,
    running: Arc<AtomicBool>,
    accept_thread: Option<thread::JoinHandle<()>>,
}

impl BrokerHandle {
    /// 通知 broker 停止并等待 accept 线程退出。
    pub fn shutdown(mut self) {
        self.running.store(false, Ordering::SeqCst);
        if let Some(h) = self.accept_thread.take() {
            let _ = h.join();
        }
    }
}

/// 在 `addr`（如 `"127.0.0.1:0"`）上启动 broker，accept 循环运行在后台线程。
pub fn spawn_broker(addr: &str) -> std::io::Result<BrokerHandle> {
    let listener = TcpListener::bind(addr)?;
    let bound = listener.local_addr()?;
    // 设为非阻塞，accept 循环才能周期性检查 running 标志以实现优雅关闭。
    listener.set_nonblocking(true)?;

    let state: Arc<Mutex<State>> = Arc::new(Mutex::new(State::default()));
    let running = Arc::new(AtomicBool::new(true));
    let sub_id_gen = Arc::new(AtomicU64::new(1));

    let accept_running = running.clone();
    let accept_thread = thread::Builder::new()
        .name("mq-broker-accept".into())
        .spawn(move || {
            eprintln!("[broker] 监听于 {bound}");
            while accept_running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, peer)) => {
                        let state = state.clone();
                        let sub_id_gen = sub_id_gen.clone();
                        // thread-per-connection：为每个连接派发一个处理线程。
                        thread::Builder::new()
                            .name(format!("mq-conn-{peer}"))
                            .spawn(move || {
                                if let Err(e) = handle_connection(stream, state, sub_id_gen) {
                                    // 连接级错误不应影响 broker：记录后关闭该连接即可。
                                    eprintln!("[broker] 连接 {peer} 结束: {e}");
                                }
                            })
                            .expect("spawn 连接线程失败");
                    }
                    // 非阻塞模式下暂时无连接：短暂休眠后再检查 running。
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                    }
                    Err(e) => {
                        eprintln!("[broker] accept 出错: {e}");
                        break;
                    }
                }
            }
            eprintln!("[broker] 已停止");
        })?;

    Ok(BrokerHandle {
        addr: bound,
        running,
        accept_thread: Some(accept_thread),
    })
}

/// 处理单个连接：循环读取请求；若为订阅请求则转入消费循环直至断开。
fn handle_connection(
    stream: TcpStream,
    state: Arc<Mutex<State>>,
    sub_id_gen: Arc<AtomicU64>,
) -> std::io::Result<()> {
    // 用 try_clone 得到读写两半：reader 负责读请求，writer 负责回响应。
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = stream;

    while let Some(req) = read_request(&mut reader)? {
        match req {
            Request::Publish { topic, payload } => {
                let offset = publish(&state, &topic, payload);
                write_response(&mut writer, &Response::Published { topic, offset })?;
            }
            Request::QueryEndOffset { topic } => {
                let offset = end_offset(&state, &topic);
                write_response(&mut writer, &Response::EndOffset { topic, offset })?;
            }
            Request::Subscribe { topic, from_offset } => {
                // 订阅后该连接就变成消费者：注册 -> 重放 -> 实时跟随，直到断开才返回。
                // 把写端（原 stream）交给消费循环，此后本函数直接返回。
                run_consumer(writer, &state, &sub_id_gen, topic, from_offset)?;
                return Ok(());
            }
        }
    }
    Ok(())
}

/// 追加一条消息到日志，并把消息扇出给所有订阅者。返回分配的 offset。
fn publish(state: &Arc<Mutex<State>>, topic: &str, payload: String) -> u64 {
    // 待发送的订阅者 Sender 列表：先在锁内收集，出锁后再发送。
    let fanout: Vec<Sender<Response>>;
    let offset;
    {
        let mut st = state.lock().unwrap();
        let log = st.topics.entry(topic.to_string()).or_default();
        offset = log.messages.len() as u64;
        log.messages.push(payload.clone());
        fanout = log.subscribers.iter().map(|(_, s)| s.clone()).collect();
    } // 全局锁在此释放。

    let msg = Response::Message {
        topic: topic.to_string(),
        offset,
        payload,
    };
    for sender in fanout {
        // send 失败说明订阅者已断开（Receiver 被 drop），忽略即可；
        // 死订阅者会在其自身连接线程退出时被清理。
        let _ = sender.send(msg.clone());
    }
    offset
}

fn end_offset(state: &Arc<Mutex<State>>, topic: &str) -> u64 {
    let st = state.lock().unwrap();
    st.topics.get(topic).map(|l| l.messages.len() as u64).unwrap_or(0)
}

/// 订阅者主循环：原子地「注册 + 快照重放」，再实时接收新消息。
fn run_consumer(
    stream: TcpStream,
    state: &Arc<Mutex<State>>,
    sub_id_gen: &Arc<AtomicU64>,
    topic: String,
    from_offset: u64,
) -> std::io::Result<()> {
    let (tx, rx): (Sender<Response>, Receiver<Response>) =
        bounded(SUBSCRIBER_CHANNEL_CAPACITY);
    let sub_id = sub_id_gen.fetch_add(1, Ordering::SeqCst);

    // 关键：在同一次加锁内完成「注册订阅者」和「快照 [from_offset, end) 的历史消息」，
    // 这样注册之后发布的消息一定会进入 rx，而快照已覆盖注册之前的消息，二者无缝衔接。
    let replay: Vec<Response> = {
        let mut st = state.lock().unwrap();
        let log = st.topics.entry(topic.clone()).or_default();
        log.subscribers.push((sub_id, tx));
        log.messages
            .iter()
            .enumerate()
            .skip(from_offset as usize)
            .map(|(off, payload)| Response::Message {
                topic: topic.clone(),
                offset: off as u64,
                payload: payload.clone(),
            })
            .collect()
    };

    let mut writer = stream;
    for msg in replay {
        write_response(&mut writer, &msg)?;
    }

    // 实时跟随：阻塞等待新消息并写给客户端；写失败说明客户端断开。
    let result = (|| -> std::io::Result<()> {
        while let Ok(msg) = rx.recv() {
            write_response(&mut writer, &msg)?;
        }
        Ok(())
    })();

    // 无论正常结束还是出错，都要把自己从订阅者列表移除，避免向死连接发送。
    if let Ok(mut st) = state.lock() {
        if let Some(log) = st.topics.get_mut(&topic) {
            log.subscribers.retain(|(id, _)| *id != sub_id);
        }
    }
    result
}
