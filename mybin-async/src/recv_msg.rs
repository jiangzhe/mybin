use crate::error::Result;
use async_net::TcpStream;
use bytes::BytesMut;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

// internal struct to concat multiple packets
#[derive(Debug)]
pub struct RecvMsg {
    pub(crate) payload_len: u32,
    pub(crate) seq_id: u8,
}

#[derive(Debug)]
enum RecvMsgState {
    ReadLen,
    ReadSeq,
    ReadPayload,
}

#[derive(Debug)]
pub struct RecvMsgFuture<'s, 'o> {
    pub(crate) stream: &'s mut TcpStream,
    state: RecvMsgState,
    out: &'o mut BytesMut,
    payload_len: u32,
    seq_id: u8,
}

impl RecvMsgFuture<'_, '_> {
    /// method to get over borrow checker
    fn stream_and_out(&mut self) -> (&mut TcpStream, &mut BytesMut) {
        (self.stream, self.out)
    }
}

impl<'s, 'o> RecvMsgFuture<'s, 'o> {
    pub(crate) fn new(stream: &'s mut TcpStream, out: &'o mut BytesMut) -> Self {
        RecvMsgFuture {
            stream,
            state: RecvMsgState::ReadLen,
            out,
            payload_len: 0,
            seq_id: 0,
        }
    }
}

impl Future for RecvMsgFuture<'_, '_> {
    type Output = Result<RecvMsg>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use bytes_parser::future::{ReadLeU24Future, ReadLenOutFuture, ReadU8Future};
        use smol::ready;

        loop {
            match self.state {
                RecvMsgState::ReadLen => {
                    // read len
                    let mut leu24_fut = ReadLeU24Future(&mut self.stream);
                    match ready!(Pin::new(&mut leu24_fut).poll(cx)) {
                        Ok(n) => {
                            self.payload_len = n;
                            self.state = RecvMsgState::ReadSeq;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                RecvMsgState::ReadSeq => {
                    // read seq
                    let mut u8_fut = ReadU8Future(&mut self.stream);
                    match ready!(Pin::new(&mut u8_fut).poll(cx)) {
                        Ok(n) => {
                            self.seq_id = n;
                            self.state = RecvMsgState::ReadPayload;
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
                RecvMsgState::ReadPayload => {
                    let n = self.payload_len as usize;
                    let (reader, out) = self.stream_and_out();
                    let mut fut = ReadLenOutFuture { reader, n, out };
                    match ready!(Pin::new(&mut fut).poll(cx)) {
                        Ok(_) => {
                            // make this future reuseable
                            self.state = RecvMsgState::ReadLen;
                            return Poll::Ready(Ok(RecvMsg {
                                payload_len: self.payload_len,
                                seq_id: self.seq_id,
                            }));
                        }
                        Err(e) => return Poll::Ready(Err(e.into())),
                    }
                }
            }
        }
    }
}
