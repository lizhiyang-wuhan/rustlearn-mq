//! 消息队列客户端：`Producer`（生产者）与 `Consumer`（消费者）。
//!
//! 两者都基于一条 TCP 连接，用 NDJSON 协议与 broker 通信。
//! 设计要点：把 socket 的读端包成 `BufReader`，写端单独持有（通过 `try_clone`），
//! 这样发送请求与读取响应互不干扰，是同步网络客户端的常见写法。

use std::io::{self, BufReader};
use std::net::TcpStream;

use super::protocol::{Request, Response, read_response, write_request};

/// 生产者：向某个 topic 追加消息。
pub struct Producer {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Producer {
    /// 连接到 broker。
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let reader = BufReader::new(stream.try_clone()?);
        Ok(Self { reader, writer: stream })
    }

    /// 发布一条消息，阻塞等待 broker 返回分配的 offset。
    ///
    /// 这是「同步 + 逐条确认」的写法，语义最清晰、最易理解（类似 Kafka 的 acks=all）。
    /// 高吞吐场景可改成异步批量发送，不等待每条确认。
    pub fn publish(&mut self, topic: &str, payload: &str) -> io::Result<u64> {
        write_request(
            &mut self.writer,
            &Request::Publish {
                topic: topic.to_string(),
                payload: payload.to_string(),
            },
        )?;
        match read_response(&mut self.reader)? {
            Some(Response::Published { offset, .. }) => Ok(offset),
            Some(Response::Error { message }) => {
                Err(io::Error::new(io::ErrorKind::Other, message))
            }
            other => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("意外响应: {other:?}"),
            )),
        }
    }

    /// 查询 topic 当前日志长度（下一条消息的 offset）。
    pub fn end_offset(&mut self, topic: &str) -> io::Result<u64> {
        write_request(&mut self.writer, &Request::QueryEndOffset { topic: topic.to_string() })?;
        match read_response(&mut self.reader)? {
            Some(Response::EndOffset { offset, .. }) => Ok(offset),
            other => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("意外响应: {other:?}"),
            )),
        }
    }
}

/// 消费者：订阅某个 topic，可先从历史 offset 重放，再实时接收。
pub struct Consumer {
    reader: BufReader<TcpStream>,
    #[allow(dead_code)] // 持有 writer 以保持连接，并用于潜在的后续请求。
    writer: TcpStream,
    /// 下一条将读到的消息 offset（仅用于展示进度）。
    pub next_offset: u64,
}

impl Consumer {
    /// 订阅 topic，`from_offset = 0` 表示从头重放全部历史消息。
    pub fn subscribe(addr: &str, topic: &str, from_offset: u64) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        let mut writer = stream.try_clone()?;
        write_request(
            &mut writer,
            &Request::Subscribe {
                topic: topic.to_string(),
                from_offset,
            },
        )?;
        let reader = BufReader::new(stream);
        Ok(Self {
            reader,
            writer,
            next_offset: from_offset,
        })
    }

    /// 阻塞读取下一条消息。连接关闭时返回 `Ok(None)`。
    pub fn recv(&mut self) -> io::Result<Option<ConsumerMessage>> {
        match read_response(&mut self.reader)? {
            Some(Response::Message { topic, offset, payload }) => {
                self.next_offset = offset + 1;
                Ok(Some(ConsumerMessage { topic, offset, payload }))
            }
            Some(Response::Error { message }) => {
                Err(io::Error::new(io::ErrorKind::Other, message))
            }
            _ => Ok(None),
        }
    }
}

/// 消费者收到的一条消息。
#[derive(Debug, Clone)]
pub struct ConsumerMessage {
    pub topic: String,
    pub offset: u64,
    pub payload: String,
}
