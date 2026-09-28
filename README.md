# kafkaexample

这是学习rust 进程间通信的代码示例，基于qwen-3.8max进行学习分析
单机「**消息队列 / 线程间通信 / 进程间通信**」的 Rust 最佳实践合集。

通过 7 个可独立运行的 demo，由浅入深地讲解 Rust 并发与 IPC 的核心概念，最终串起一个 **mini Kafka 式消息队列**，涵盖 append-only log、offset、发布/订阅、历史重放、实时推送等关键机制。

---

## 目录

- [快速开始](#快速开始)
- [项目结构](#项目结构)
- [Demo 一览](#demo-一览)
  - [线程间通信](#线程间通信)
  - [进程间通信](#进程间通信)
  - [消息队列](#消息队列)
- [消息队列架构](#消息队列架构)
  - [核心概念](#核心概念)
  - [模块划分](#模块划分)
  - [并发模型](#并发模型)
  - [数据流](#数据流)
  - [线协议](#线协议)
- [建议学习路线](#建议学习路线)

---

## 快速开始

```bash
# 克隆仓库
git clone <repo-url> && cd kafkaexample

# 查看所有 demo 列表
cargo run

# 运行任意一个 demo
cargo run --bin thread_mpsc
cargo run --bin mq_demo

# 启动独立 broker（默认监听 127.0.0.1:9092）
cargo run --bin mq_broker
cargo run --bin mq_broker -- 127.0.0.1:19092   # 自定义地址
```

**依赖**：Rust 2024 edition，仅需 `std` + `serde` + `crossbeam`，无重型框架。

---

## 项目结构

```
kafkaexample/
├── Cargo.toml
├── src/
│   ├── lib.rs                  # 库入口，声明 mq 模块
│   ├── main.rs                 # 索引菜单，列出所有 demo
│   ├── mq/                     # ★ 迷你 Kafka 消息队列核心
│   │   ├── mod.rs              # 模块入口 & re-export
│   │   ├── protocol.rs         # 线协议：NDJSON 请求/响应
│   │   ├── broker.rs           # 服务端：topic 日志 + 订阅扇出
│   │   └── client.rs           # 客户端：Producer / Consumer
│   └── bin/                    # 可执行 demo
│       ├── thread_mpsc.rs          # Demo 1: std mpsc 通道
│       ├── thread_shared_state.rs  # Demo 2: Arc + Mutex/RwLock/Atomic
│       ├── thread_crossbeam.rs     # Demo 3: crossbeam 作用域线程/mpmc/select
│       ├── ipc_unix_socket.rs      # Demo 4: Unix Domain Socket IPC
│       ├── ipc_child_pipe.rs       # Demo 5: 父子进程管道
│       ├── mq_broker.rs            # Demo 6: 独立运行的 broker
│       └── mq_demo.rs              # Demo 7: 端到端消息队列演示
└── target/
```

---

## Demo 一览

### 线程间通信

| Demo | 运行命令 | 核心知识点 |
|------|---------|-----------|
| **thread_mpsc** | `cargo run --bin thread_mpsc` | `std::sync::mpsc` 多生产者单消费者通道；「不要通过共享内存来通信，而要通过通信来共享内存」 |
| **thread_shared_state** | `cargo run --bin thread_shared_state` | `Arc<AtomicU64>` 无锁计数、`Arc<Mutex<T>>` 互斥访问、`Arc<RwLock<T>>` 多读单写；临界区最小化原则 |
| **thread_crossbeam** | `cargo run --bin thread_crossbeam` | crossbeam 作用域线程（栈借用，无需 Arc）、mpmc 工作池、`select!` 多路复用、有界通道背压 |

### 进程间通信

| Demo | 运行命令 | 核心知识点 |
|------|---------|-----------|
| **ipc_unix_socket** | `cargo run --bin ipc_unix_socket` | Unix Domain Socket 双向通信；自启动子进程作服务端；socket 文件生命周期管理 |
| **ipc_child_pipe** | `cargo run --bin ipc_child_pipe` | `Stdio::piped()` 父子进程 stdin/stdout 管道；shell `a | b` 的底层原理；避免管道死锁 |

### 消息队列

| Demo | 运行命令 | 核心知识点 |
|------|---------|-----------|
| **mq_broker** | `cargo run --bin mq_broker` | 独立运行的 broker 进程，类似 `kafka-server-start`；KRaft 风格单进程，不依赖 ZooKeeper |
| **mq_demo** | `cargo run --bin mq_demo` | 端到端演示：进程内启动 broker + Producer + 2 个 Consumer，展示发布/订阅、offset、重放、实时跟随 |

---

## 消息队列架构

### 核心概念

借鉴 Kafka 的设计，每个 **topic** 是一条 **append-only（只追加）的日志**：

```
Broker
├── topic "orders"    → [msg₀, msg₁, msg₂, ...]   offset: 0, 1, 2, ...
├── topic "payments"  → [msg₀, msg₁, ...]          offset: 0, 1, ...
└── topic "logs"      → [msg₀, msg₁, msg₂, msg₃]   offset: 0, 1, 2, 3
```

- **生产者（Producer）** — 向 topic 末尾追加消息，获得 broker 分配的单调递增 `offset`
- **消费者（Consumer）** — 从任意 offset 开始读取，先**重放**历史消息，再**实时跟随**新消息
- 消息**不因被消费而删除**，可重复消费 — 这是 Kafka 区别于传统 MQ 的关键

### 模块划分

```
┌──────────────────────────────────────────────────────────────┐
│                        mq 模块                               │
│                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐      │
│  │  protocol   │    │   broker    │    │   client    │      │
│  │             │    │             │    │             │      │
│  │ Request     │◀──▶│ State       │◀──▶│ Producer    │      │
│  │ Response    │    │ TopicLog    │    │ Consumer    │      │
│  │ NDJSON 成帧 │    │ publish()   │    │ publish()   │      │
│  │             │    │ run_consumer│    │ subscribe() │      │
│  └─────────────┘    │ spawn_broker│    │ recv()      │      │
│                     └─────────────┘    └─────────────┘      │
└──────────────────────────────────────────────────────────────┘
```

| 模块 | 文件 | 职责 |
|------|------|------|
| **protocol** | `protocol.rs` | 线协议层 — `Request`/`Response` 枚举，NDJSON（换行分隔 JSON）成帧 |
| **broker** | `broker.rs` | 服务端 — 全局状态管理、topic 日志维护、订阅者注册与消息扇出 |
| **client** | `client.rs` | 客户端 — `Producer`（同步发布 + 确认）和 `Consumer`（订阅 + 重放 + 实时接收）|

### 并发模型

```
┌──────────┐  ┌──────────┐  ┌──────────┐
│ Client A │  │ Client B │  │ Client C │
│(Producer)│  │(Consumer)│  │(Consumer)│
└────┬─────┘  └────┬─────┘  └────┬─────┘
     │              │              │
     └──────┬───────┴──────────────┘
            │
   Arc<Mutex<State>>          ← 全局锁，临界区极短
            │
   ┌────────┴────────┐
   │   TopicLog      │
   │   messages: Vec │       ← append-only 日志
   │   subscribers ──┼──▶ crossbeam channel ──▶ Consumer B
   │                 └──▶ crossbeam channel ──▶ Consumer C
   └─────────────────┘
```

关键设计决策：

| 决策 | 原因 |
|------|------|
| **thread-per-connection** | 每连接一个线程，简单直观，适合教学 |
| **Mutex 临界区极短** | 锁内只做数据操作（追加日志 + 克隆 Sender），**锁外才做 I/O**，避免慢消费者阻塞全局 |
| **crossbeam 有界通道** | 容量 1024，满时阻塞发送方，提供**背压**防止内存撑爆 |
| **原子注册** | 订阅注册 + 历史快照在**同一次加锁**内完成，保证重放与实时推送无缝衔接（不丢不重） |
| **非阻塞 accept** | accept 循环周期性检查 `AtomicBool` 标志，实现优雅关闭 |

### 数据流

#### 发布流程

```
Producer                    Broker                     Consumer(s)
   │                          │                            │
   │── Publish{topic,payload}─▶│                            │
   │                          │── lock(): 追加日志           │
   │                          │          克隆所有 Sender    │
   │                          │── unlock()                  │
   │◀── Published{offset} ────│                            │
   │                          │── 锁外: channel.send() ────▶│ 收到 Message
```

#### 订阅流程

```
Consumer                    Broker
   │                          │
   │── Subscribe{topic, off}──▶│
   │                          │── lock(): 注册订阅者
   │                          │          快照历史 [off, end)
   │                          │── unlock()
   │◀── Message{off} ─────────│   ← 重放阶段
   │◀── Message{off+1} ───────│
   │    ...                   │
   │◀── Message{new} ─────────│   ← 实时跟随（通过 channel 推送）
```

### 线协议

采用 **NDJSON**（换行分隔的 JSON）：每条消息序列化为单行 JSON，以 `\n` 结尾。

```jsonc
// Client → Broker (Request)
{"op": "publish",   "topic": "orders", "payload": "order-123"}
{"op": "subscribe", "topic": "orders", "from_offset": 0}
{"op": "query_end_offset", "topic": "orders"}

// Broker → Client (Response)
{"event": "published", "topic": "orders", "offset": 3}
{"event": "message",   "topic": "orders", "offset": 3, "payload": "order-123"}
{"event": "end_offset","topic": "orders", "offset": 5}
{"event": "error",     "message": "..."}
```

选择 NDJSON 的原因：人类可读、跨语言、用 `BufRead::read_line` 即可成帧，非常适合教学。生产环境通常改用二进制协议（如 Kafka 原生协议或 bincode + 长度前缀）以追求更高吞吐。

---

## 建议学习路线

```
thread_mpsc          理解通道 = 所有权转移
       ↓
thread_shared_state  理解 Arc + 内部可变性（Mutex/RwLock/Atomic）
       ↓
thread_crossbeam     掌握 mpmc、作用域线程、select、背压
       ↓
ipc_unix_socket      跨进程通信：UDS 双向通信
       ↓
ipc_child_pipe       跨进程通信：stdin/stdout 管道
       ↓
mq_broker + mq_demo  ★ 串起所有概念：线程 + 通道 + 共享状态 + TCP → 消息队列
```

每个 demo 源码中都包含大量中文注释，建议边读代码边运行观察输出。
