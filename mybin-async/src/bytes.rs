use smol::io::AsyncRead;
use smol::ready;
use crate::error::{Result, Error, Needed};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::io::ErrorKind;

pub trait AsyncReadBytes: AsyncRead {

    fn take_out<'a>(&'a mut self, total: usize, out: &'a mut Vec<u8>) -> TakeOutFuture<'a, Self> 
    where
        Self: Unpin,
    {
        TakeOutFuture{
            reader: self,
            total,
            out,
        }
    }

    fn take<'a>(&'a mut self, total: usize) -> TakeFuture<'a, Self>
    where
        Self: Unpin,
    {
        TakeFuture{
            reader: self,
            total,
        }
    }

    fn take_until<'a>(&'a mut self, b: u8, include: bool) -> TakeUntilFuture<'a, Self>
    where
        Self: Unpin,
    {
        TakeUntilFuture{
            reader: self,
            b,
            include,
        }
    }
}

impl<R: AsyncRead> AsyncReadBytes for R {}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TakeOutFuture<'a, T: Unpin + ?Sized> {
    reader: &'a mut T,
    total: usize,
    out: &'a mut Vec<u8>,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for TakeOutFuture<'_, R> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            reader,
            total,
            out,
        } = &mut *self;
        if *total == 0 {
            return Poll::Ready(Ok(()));
        }
        take_out_internal(reader, cx, *total, out)
    }
}

fn take_out_internal<'a, R: AsyncRead + Unpin + ?Sized>(
    reader: &'a mut R,
    cx: &mut Context<'_>,
    required: usize,
    out: &'a mut Vec<u8>,
) -> Poll<Result<()>> {
    struct Guard<'a> {
        out: &'a mut Vec<u8>,
        len: usize,
    }
    impl Drop for Guard<'_> {
        fn drop(&mut self) {
            self.out.resize(self.len, 0);
        }
    }
    let len = out.len();
    let mut g = Guard{
        out,
        len,
    };
    let mut read = 0;
    g.out.resize(g.len + required, 0);
    let mut reader = Pin::new(reader);
    loop {
        match ready!(reader.as_mut().poll_read(cx, &mut g.out[g.len..])) {
            Ok(0) => return Poll::Ready(Err(Error::InputIncomplete(vec![], Needed::Size(required - read)))),
            Ok(n) if read + n == required => {
                g.len += n;
                return Poll::Ready(Ok(()));
            }
            Ok(n) => {
                read += n;
                g.len += n;
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
            Err(e) => return Poll::Ready(Err(Error::from(e))),
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TakeFuture<'a, T: Unpin + ?Sized> {
    reader: &'a mut T,
    total: usize,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for TakeFuture<'_, R> {
    type Output = Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
         let Self {
            reader,
            total,
        } = &mut *self;
        if *total == 0 {
            return Poll::Ready(Ok(Vec::new()));
        }
        let mut out = Vec::new();
        match ready!(take_out_internal(reader, cx, *total, &mut out)) {
            Ok(..) => Poll::Ready(Ok(out)),
            Err(Error::InputIncomplete(_, needed)) => Poll::Ready(Err(Error::InputIncomplete(out, needed))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TakeUntilFuture<'a, T: Unpin + ?Sized> {
    reader: &'a mut T,
    b: u8,
    include: bool,
}

impl<R: AsyncRead + Unpin + ?Sized> Future for TakeUntilFuture<'_, R> {
    type Output = Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Self {
            reader,
            b,
            include,
        } = &mut *self;
        take_until_internal(reader, cx, *b, *include)
    }
}

fn take_until_internal<'a, R: AsyncRead + Unpin + ?Sized>(
    reader: &'a mut R,
    cx: &mut Context<'_>,
    b0: u8,
    include: bool,
) -> Poll<Result<Vec<u8>>> {
    let mut reader = Pin::new(reader);
    let mut b = 0u8;
    let mut bs = Vec::new();
    loop {
        match ready!(reader.as_mut().poll_read(cx, std::slice::from_mut(&mut b))) {
            Ok(0) => return Poll::Ready(Err(Error::InputIncomplete(bs, Needed::Unknown))),
            Ok(..) if b == b0 => {
                if include {
                    bs.push(b);
                }
                return Poll::Ready(Ok(bs));
            }
            Ok(..) => {
                bs.push(b);
            }
            Err(ref e) if e.kind() == ErrorKind::Interrupted => (),
            Err(e) => return Poll::Ready(Err(Error::from(e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[smol_potat::test]
    async fn test_take_out_0() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let mut out = vec![];
        let _ = reader.take_out(0, &mut out).await.unwrap();
        assert_eq!(Vec::<u8>::new(), out);
    }

    #[smol_potat::test]
    async fn test_take_out_3() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let mut out = vec![];
        let _ = reader.take_out(3, &mut out).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], out);
    }

    #[smol_potat::test]
    async fn test_take_out_6() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let mut out = vec![];
        let rst = reader.take_out(6, &mut out).await;
        dbg!(&rst);
        dbg!(out);
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_take_0() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let out = reader.take(0).await.unwrap();
        assert_eq!(Vec::<u8>::new(), out);
    }

    #[smol_potat::test]
    async fn test_take_3() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let out = reader.take(3).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], out);
    }

    #[smol_potat::test]
    async fn test_take_6() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let rst = reader.take(6).await;
        dbg!(&rst);
        assert!(rst.is_err());
    }

    #[smol_potat::test]
    async fn test_take_until_3_inclusive() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let rs = reader.take_until(3, true).await.unwrap();
        assert_eq!(vec![1u8, 2, 3], rs);
    }

    #[smol_potat::test]
    async fn test_take_until_3_exclusive() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let rs = reader.take_until(3, false).await.unwrap();
        assert_eq!(vec![1u8, 2], rs);
    }

    #[smol_potat::test]
    async fn test_take_until_6() {
        let bs = [1u8,2,3,4,5];
        let mut reader = &bs[..];
        let rs = reader.take_until(3, false).await.unwrap();
        assert_eq!(vec![1u8, 2], rs);
    }
}