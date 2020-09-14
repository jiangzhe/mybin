use crate::conn::Conn;
use crate::error::{Error, Result};
use bytes::{Buf, Bytes};
use futures::{ready, AsyncRead, AsyncWrite};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug, Clone, Copy)]
pub enum MsgState {
    Len,
    Seq,
    Payload,
}

#[derive(Debug)]
pub struct RecvMsgFuture<'s, S> {
    conn: &'s mut Conn<S>,
}

impl<'s, S> RecvMsgFuture<'s, S>
where
    S: AsyncRead + Unpin,
{
    pub(crate) fn new(conn: &'s mut Conn<S>) -> Self {
        RecvMsgFuture { conn }
    }
}

impl<R> Future for RecvMsgFuture<'_, R>
where
    R: AsyncRead + Unpin,
{
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use bytes_parser::future::{ReadLeU24Future, ReadLenFuture, ReadU8Future};

        loop {
            match self.conn.msg_state {
                MsgState::Len => {
                    // read len
                    let mut len_fut = ReadLeU24Future::new(&mut self.conn.stream);
                    match ready!(Pin::new(&mut len_fut).poll(cx)) {
                        Ok(n) => {
                            assert!(n <= 0xffffff);
                            self.conn.recv_len = n;
                            self.conn.msg_state = MsgState::Seq;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Seq => {
                    // read seq
                    let mut seq_fut = ReadU8Future(&mut self.conn.stream);
                    match ready!(Pin::new(&mut seq_fut).poll(cx)) {
                        Ok(n) => {
                            if n != self.conn.pkt_nr {
                                return Poll::Ready(Err(Error::PacketError(format!(
                                    "Get server packet out of order: {} != {}",
                                    n, self.conn.pkt_nr
                                ))));
                            }
                            if n == 0xff {
                                self.conn.pkt_nr = 0;
                            } else {
                                self.conn.pkt_nr = n + 1;
                            }
                            self.conn.msg_state = MsgState::Payload;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Payload => {
                    let Self {
                        conn,
                        // out,
                        // total_len,
                        // curr_len,
                    } = &mut *self;
                    let mut fut = ReadLenFuture::new(&mut conn.stream, conn.recv_len as usize);
                    match ready!(Pin::new(&mut fut).poll(cx)) {
                        Ok(buf) => {
                            if conn.recv_len < 0xffffff {
                                // make this future reuseable
                                conn.msg_state = MsgState::Len;
                                log::debug!(
                                    "completed packet: len={}, pkt_nr={}",
                                    buf.len(),
                                    conn.pkt_nr,
                                );
                                if conn.recv_buf.is_empty() {
                                    return Poll::Ready(Ok(Bytes::from(buf)));
                                }
                                conn.recv_buf.extend_from_slice(&buf);
                                return Poll::Ready(Ok(Bytes::from(std::mem::replace(
                                    &mut conn.recv_buf,
                                    vec![],
                                ))));
                            }
                            // same as max size, must have
                            // one additional packet even if empty
                            assert_eq!(0xffffff, conn.recv_len);
                            conn.recv_buf.extend_from_slice(&buf);
                            conn.msg_state = MsgState::Len;
                        }
                        Err(e) => {
                            return Poll::Ready(Err(e.into()));
                        }
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct SendMsgFuture<'s, S> {
    conn: &'s mut Conn<S>,
    // bs: Bytes,
    // state: MsgState,
}

impl<'s, S> SendMsgFuture<'s, S>
where
    S: AsyncWrite + Unpin,
{
    pub fn new(conn: &'s mut Conn<S>) -> Self {
        Self { conn }
    }
}

impl<'s, S> Future for SendMsgFuture<'s, S>
where
    S: AsyncWrite + Unpin,
{
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use bytes_parser::future::{WriteBytesFuture, WriteLeU24Future, WriteU8Future};

        if self.conn.send_buf.is_empty() {
            return Poll::Ready(Ok(()));
        }
        loop {
            match self.conn.msg_state {
                MsgState::Len => {
                    // write len as u24
                    let n = usize::min(
                        self.conn.send_buf.last().as_ref().unwrap().remaining(),
                        0xffffff,
                    ) as u32;
                    let mut len_fut = WriteLeU24Future {
                        writer: &mut self.conn.stream,
                        n,
                    };
                    match ready!(Pin::new(&mut len_fut).poll(cx)) {
                        Ok(_) => {
                            self.conn.msg_state = MsgState::Seq;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Seq => {
                    // write seq as u8
                    let n = self.conn.pkt_nr;
                    let mut seq_fut = WriteU8Future {
                        writer: &mut self.conn.stream,
                        n,
                    };
                    match ready!(Pin::new(&mut seq_fut).poll(cx)) {
                        Ok(_) => {
                            self.conn.msg_state = MsgState::Payload;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Payload => {
                    if self.conn.send_buf.is_empty() {
                        return Poll::Ready(Ok(()));
                    }
                    let len = usize::min(
                        self.conn.send_buf.last().as_ref().unwrap().remaining(),
                        0xffffff,
                    );
                    if len == 0xffffff {
                        let head = self.conn.send_buf.last_mut().unwrap().split_to(0xffffff);
                        self.conn.send_buf.push(head);
                    }
                    let Conn {
                        stream, send_buf, ..
                    } = &mut *self.conn;
                    let mut payload_fut = WriteBytesFuture {
                        writer: stream,
                        bs: send_buf.last_mut().unwrap(),
                    };
                    match ready!(Pin::new(&mut payload_fut).poll(cx)) {
                        Ok(_) => {
                            assert!(!self.conn.send_buf.last().as_ref().unwrap().has_remaining());
                            self.conn.send_buf.pop();
                            self.conn.pkt_nr += 1;
                            log::debug!("pkt_nr set to {}", self.conn.pkt_nr);
                            self.conn.msg_state = MsgState::Len;
                            if self.conn.send_buf.is_empty() {
                                return Poll::Ready(Ok(()));
                            }
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
            }
        }
    }
}
