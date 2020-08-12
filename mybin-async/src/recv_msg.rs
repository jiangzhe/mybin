use std::future::Future;
use std::task::{Poll, Context};
use std::pin::Pin;
use async_net::TcpStream;
use crate::error::Result;

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
pub struct RecvMsgFuture<'a> {
    pub(crate) stream: &'a mut TcpStream,
    state: RecvMsgState,
    out: &'a mut Vec<u8>,
    payload_len: u32,
    seq_id: u8,
}

impl RecvMsgFuture<'_> {
    /// method to get over borrow checker
    fn stream_and_out(&mut self) -> (&mut TcpStream, &mut Vec<u8>) {
        (self.stream, self.out)
    }
}

impl<'a> RecvMsgFuture<'a> {
    pub(crate) fn new(stream: &'a mut TcpStream, out: &'a mut Vec<u8>) -> Self {
        RecvMsgFuture{
            stream,
            state: RecvMsgState::ReadLen,
            out,
            payload_len: 0,
            seq_id: 0,
        }
    }
}

impl Future for RecvMsgFuture<'_> {
    type Output = Result<RecvMsg>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        use smol::ready;
        use crate::number::{ReadU8Future, ReadLeU24Future};
        use crate::bytes::TakeOutFuture;

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
                        Err(e) => return Poll::Ready(Err(e)),
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
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                RecvMsgState::ReadPayload => {
                    let total = self.payload_len as usize;
                    let (reader, out) = self.stream_and_out();
                    let mut take_out_fut = TakeOutFuture{
                        reader,
                        total,
                        out,
                    };
                    match ready!(Pin::new(&mut take_out_fut).poll(cx)) {
                        Ok(_) => {
                            // make this future reuseable
                            self.state = RecvMsgState::ReadLen;
                            return Poll::Ready(Ok(RecvMsg{
                                payload_len: self.payload_len,
                                seq_id: self.seq_id,
                            }));
                        },
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
            }
        }
    }
}
