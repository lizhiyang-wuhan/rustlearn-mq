//! 客户端与 broker 之间的线协议。
//!
//! 采用 **换行分隔的 JSON（NDJSON）**：每条消息序列化成一行 JSON，以 `\n` 结尾。
//! 选择它的原因：人类可读、跨语言、用 `BufRead::read_line` 就能成帧，非常适合教学。
//! （生产环境为追求吞吐常改用二进制成帧，如 Kafka 自己的协议或 bincode + 长度前缀。）

use std::io::{self, BufRead, Write};

use serde::{Deserialize, Serialize};

/// 客户端 -> broker 的请求。
///
/// `#[serde(tag = "op")]` 让枚举序列化为 `{"op": "publish", ...}` 这种带类型标签的对象，
/// 便于阅读和调试。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// 追加一条消息到 topic 日志末尾。
    Publish { topic: String, payload: String },
    /// 订阅 topic：先从 `from_offset` 重放历史，再实时接收新消息。
    Subscribe { topic: String, from_offset: u64 },
    /// 查询 topic 当前日志长度（即下一条消息将获得的 offset）。
    QueryEndOffset { topic: String },
}

/// broker -> 客户端的响应 / 事件。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum Response {
    /// publish 成功，返回该消息被分配的 offset。
    Published { topic: String, offset: u64 },
    /// QueryEndOffset 的结果。
    EndOffset { topic: String, offset: u64 },
    /// 推送给订阅者的一条消息（重放或实时）。
    Message {
        topic: String,
        offset: u64,
        payload: String,
    },
    /// 错误信息。
    Error { message: String },
}

/// 把请求写成一行 JSON 并 flush。
pub fn write_request<W: Write>(w: &mut W, req: &Request) -> io::Result<()> {
    write_line(w, &serde_json::to_string(req).expect("Request 序列化不应失败"))
}

/// 把响应写成一行 JSON 并 flush。
pub fn write_response<W: Write>(w: &mut W, resp: &Response) -> io::Result<()> {
    write_line(w, &serde_json::to_string(resp).expect("Response 序列化不应失败"))
}

fn write_line<W: Write>(w: &mut W, line: &str) -> io::Result<()> {
    w.write_all(line.as_bytes())?;
    w.write_all(b"\n")?;
    // 教学场景下每条消息立即 flush，保证对端能马上收到；
    // 高吞吐场景应改为批量写入以减少 syscall。
    w.flush()
}

/// 从 reader 读取下一行并解析为 Request；连接正常关闭时返回 `Ok(None)`。
pub fn read_request<R: BufRead>(r: &mut R) -> io::Result<Option<Request>> {
    read_line_json(r)
}

/// 从 reader 读取下一行并解析为 Response；连接正常关闭时返回 `Ok(None)`。
pub fn read_response<R: BufRead>(r: &mut R) -> io::Result<Option<Response>> {
    read_line_json(r)
}

fn read_line_json<R: BufRead, T: for<'de> Deserialize<'de>>(r: &mut R) -> io::Result<Option<T>> {
    let mut buf = String::new();
    let n = r.read_line(&mut buf)?;
    if n == 0 {
        // EOF：对端关闭了连接。
        return Ok(None);
    }
    match serde_json::from_str::<T>(buf.trim_end()) {
        Ok(v) => Ok(Some(v)),
        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidData, e)),
    }
}
