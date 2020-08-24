use crate::conn::Conn;
use crate::error::{Error, Result};
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::WriteToBytes;
use futures::{ready, AsyncRead, AsyncWrite};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// internal struct to concat multiple packets
// #[derive(Debug)]
// pub struct RecvMsg {
//     pub total_len: usize,
//     // next pkt_nr
//     pub pkt_nr: u8,
// }

#[derive(Debug, Clone, Copy)]
enum MsgState {
    Len,
    Seq,
    Payload,
}

#[derive(Debug)]
pub struct RecvMsgFuture<'s, S> {
    pub(crate) conn: &'s mut Conn<S>,
    state: MsgState,
    out: BytesMut,
    curr_len: u32,
    total_len: usize,
}

impl<'s, S> RecvMsgFuture<'s, S>
where
    S: AsyncRead + Unpin,
{
    pub(crate) fn new(conn: &'s mut Conn<S>) -> Self {
        RecvMsgFuture {
            conn,
            state: MsgState::Len,
            out: BytesMut::new(),
            curr_len: 0,
            total_len: 0,
        }
    }
}

impl<R> Future for RecvMsgFuture<'_, R>
where
    R: AsyncRead + Unpin,
{
    type Output = Result<Bytes>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use bytes_parser::future::{ReadLeU24Future, ReadLenOutFuture, ReadU8Future};

        loop {
            match self.state {
                MsgState::Len => {
                    // read len
                    let mut len_fut = ReadLeU24Future(&mut self.conn.stream);
                    match ready!(Pin::new(&mut len_fut).poll(cx)) {
                        Ok(n) => {
                            assert!(n <= 0xffffff);
                            self.curr_len = n;
                            self.state = MsgState::Seq;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Seq => {
                    // read seq
                    let mut seq_fut = ReadU8Future(&mut self.conn.stream);
                    match ready!(Pin::new(&mut seq_fut).poll(cx)) {
                        Ok(n) => {
                            // self.conn.pkt_nr = n;
                            if n != self.conn.pkt_nr {
                                return Poll::Ready(Err(Error::PacketError(format!(
                                    "Get server packet out of order: {} != {}",
                                    n, self.conn.pkt_nr
                                ))));
                            }
                            self.conn.pkt_nr += 1;
                            self.state = MsgState::Payload;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Payload => {
                    let Self {
                        conn,
                        out,
                        state,
                        total_len,
                        curr_len,
                    } = &mut *self;
                    let mut fut = ReadLenOutFuture {
                        reader: &mut conn.stream,
                        n: *curr_len as usize,
                        out,
                    };
                    match ready!(Pin::new(&mut fut).poll(cx)) {
                        Ok(_) => {
                            *total_len += *curr_len as usize;
                            if *curr_len < 0xffffff {
                                // make this future reuseable
                                *state = MsgState::Len;
                                log::debug!(
                                    "completed packet: total_len={}, pkt_nr={}",
                                    total_len,
                                    conn.pkt_nr
                                );
                                let msg = out.split_to(out.remaining()).freeze();
                                return Poll::Ready(Ok(msg));
                            }

                            // same as max size, must have
                            // one additional packet even if empty
                            assert_eq!(0xffffff, *curr_len);
                            *state = MsgState::Len;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct SendMsgFuture<'s, S> {
    pub(crate) conn: &'s mut Conn<S>,
    bs: Bytes,
    state: MsgState,
}

impl<'s, S> SendMsgFuture<'s, S>
where
    S: AsyncWrite + Unpin,
{
    pub fn new<T>(conn: &'s mut Conn<S>, msg: T) -> Self
    where
        T: WriteToBytes,
    {
        let mut buf = BytesMut::new();
        // won't fail to append bytes to buffer
        msg.write_to(&mut buf).unwrap();
        let bs = buf.freeze();
        Self::new_bytes(conn, bs)
    }

    pub fn new_bytes(conn: &'s mut Conn<S>, bs: Bytes) -> Self {
        SendMsgFuture {
            conn,
            bs,
            state: MsgState::Len,
        }
    }
}

impl<'s, S> Future for SendMsgFuture<'s, S>
where
    S: AsyncWrite + Unpin,
{
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use bytes_parser::future::{WriteBytesFuture, WriteLeU24Future, WriteU8Future};

        if !self.bs.has_remaining() {
            return Poll::Ready(Ok(()));
        }
        loop {
            match self.state {
                MsgState::Len => {
                    // write len as u24
                    let n = usize::min(self.bs.remaining(), 0xffffff) as u32;
                    let mut len_fut = WriteLeU24Future {
                        writer: &mut self.conn.stream,
                        n,
                    };
                    match ready!(Pin::new(&mut len_fut).poll(cx)) {
                        Ok(_) => {
                            self.state = MsgState::Seq;
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
                            self.state = MsgState::Payload;
                            self.conn.pkt_nr += 1;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                MsgState::Payload => {
                    if !self.bs.has_remaining() {
                        return Poll::Ready(Ok(()));
                    }
                    let len = usize::min(self.bs.remaining(), 0xffffff);
                    let end = len < 0xffffff;
                    let mut to_send = self.bs.split_to(len);
                    let mut payload_fut = WriteBytesFuture {
                        writer: &mut self.conn.stream,
                        bs: &mut to_send,
                    };
                    match ready!(Pin::new(&mut payload_fut).poll(cx)) {
                        Ok(_) => {
                            self.state = MsgState::Len;
                            if end {
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
